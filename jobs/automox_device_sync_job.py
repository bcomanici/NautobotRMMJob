"""
Nautobot Job: Sync Automox devices into Nautobot Devices.
"""

from __future__ import annotations

import json
import re
from datetime import date
from typing import Any, Dict, Iterable, List, Optional

import requests
from django.utils.text import slugify

from nautobot.apps.jobs import BooleanVar, IntegerVar, Job, ObjectVar, StringVar, register_jobs
from nautobot.dcim.models import Device, DeviceType, Location, Manufacturer, Platform, SoftwareVersion
from nautobot.extras.choices import SecretsGroupAccessTypeChoices, SecretsGroupSecretTypeChoices
from nautobot.extras.models import Role, SecretsGroup, Status
from nautobot.virtualization.models import VirtualMachine

name = "RMM Integrations"


class SyncAutomoxDevices(Job):
    """Import or update Nautobot Devices from Automox server inventory."""

    secrets_group = ObjectVar(
        model=SecretsGroup,
        description="Secrets Group containing Automox username, password/API key, and organization ID/key.",
    )

    default_location = ObjectVar(
        model=Location,
        description="Location assigned to newly created Automox devices.",
    )

    device_role = ObjectVar(
        model=Role,
        description="Role assigned to newly created Automox devices.",
        query_params={"content_types": "dcim.device"},
    )

    device_status = ObjectVar(
        model=Status,
        description="Status assigned to created/updated Automox devices.",
        query_params={"content_types": "dcim.device"},
    )

    automox_base_url = StringVar(
        description="Automox API base URL.",
        default="https://console.automox.com/api",
        required=True,
    )

    automox_org_query_param = StringVar(
        description="Automox organization query parameter name. Automox commonly uses 'o'.",
        default="o",
        required=True,
    )

    request_page_size = IntegerVar(
        description="Number of Automox devices to request per page where supported.",
        default=500,
        min_value=1,
        max_value=1000,
    )

    create_missing_device_types = BooleanVar(
        description="Create missing Manufacturer and DeviceType records from Automox detail.VENDOR/detail.MODEL.",
        default=True,
    )

    update_existing_devices = BooleanVar(
        description="Update existing Nautobot devices matched by hostname/name.",
        default=True,
    )

    set_rmm_field = BooleanVar(
        description="Set the Nautobot custom field rmm to 'Automox'.",
        default=True,
    )

    class Meta:
        name = "Sync Devices from Automox"
        description = "Pull Automox device inventory into Nautobot Devices and RMM custom fields."
        has_sensitive_variables = False
        soft_time_limit = 900
        time_limit = 1200
        field_order = [
            "secrets_group",
            "default_location",
            "device_role",
            "device_status",
            "automox_base_url",
            "automox_org_query_param",
            "request_page_size",
            "create_missing_device_types",
            "update_existing_devices",
            "set_rmm_field",
        ]

    def run(
        self,
        *,
        secrets_group: SecretsGroup,
        default_location: Location,
        device_role: Role,
        device_status: Status,
        automox_base_url: str,
        automox_org_query_param: str,
        request_page_size: int,
        create_missing_device_types: bool,
        update_existing_devices: bool,
        set_rmm_field: bool,
    ) -> str:
        automox_username = self._get_secret(secrets_group, "username", required=False)
        automox_api_key = self._get_secret(secrets_group, "password", required=True)
        automox_org_key = self._get_secret(secrets_group, "secret", required=True)

        if automox_username:
            self.logger.info("Using Automox credential set for: %s", automox_username)

        records = self._fetch_automox_servers(
            base_url=automox_base_url,
            api_key=automox_api_key,
            org_query_param=automox_org_query_param,
            org_key=automox_org_key,
            page_size=request_page_size,
        )

        self.logger.info("Fetched %s Automox device records.", len(records))

        created = 0
        updated = 0
        skipped = 0
        skipped_vms = 0

        for record in records:
            raw_hostname = self._hostname(record)
            hostname = self._normalize_hostname(raw_hostname)

            if not hostname:
                skipped += 1
                self.logger.warning(
                    "Skipping Automox record without usable hostname/name: %s",
                    self._safe_record_id(record),
                )
                continue

            if self._find_virtual_machine_by_hostname(record, raw_hostname, hostname):
                skipped += 1
                skipped_vms += 1
                self.logger.info(
                    "Skipping Automox record %s because it exists as a Nautobot VirtualMachine.",
                    raw_hostname,
                )
                continue

            device = self._find_device_by_hostname(record, raw_hostname, hostname)

            if device and not update_existing_devices:
                skipped += 1
                self.logger.info(
                    "Skipping existing device because updates are disabled: %s",
                    hostname,
                )
                continue

            manufacturer_name = self._manufacturer_name(record)
            model_name = self._model_name(record)
            serial = self._serial_number(record)

            device_type = self._get_or_create_device_type(
                manufacturer_name=manufacturer_name,
                model_name=model_name,
                create_missing=create_missing_device_types,
            )

            if device_type is None:
                skipped += 1
                self.logger.warning(
                    "Skipping %s because DeviceType %s/%s does not exist and creation is disabled.",
                    hostname,
                    manufacturer_name,
                    model_name,
                )
                continue

            platform = self._get_or_create_platform(record)
            software_version = self._get_or_create_software_version(record, platform=platform)

            custom_fields = self._custom_fields_from_automox(
                record,
                set_rmm_field=set_rmm_field,
            )

            self.logger.info(
                "Automox mapped fields for %s: manufacturer=%s model=%s platform=%s software_version=%s cpu=%s installed_ram=%s",
                hostname,
                manufacturer_name,
                model_name,
                platform.name if platform else "",
                software_version.version if software_version else "",
                custom_fields.get("cpu"),
                custom_fields.get("installed_ram"),
            )

            if device is None:
                device = Device(
                    name=hostname,
                    location=default_location,
                    role=device_role,
                    status=device_status,
                    device_type=device_type,
                    serial=serial,
                )
                device.validated_save()

                device.device_type = device_type
                device.serial = serial or device.serial

                if platform is not None:
                    device.platform = platform

                if software_version is not None and self._model_has_field(Device, "software_version"):
                    device.software_version = software_version

                self._apply_custom_fields(device, custom_fields)
                device.validated_save()

                created += 1
                self.logger.info(
                    "Created device %s with DeviceType %s/%s from Automox hostname %s.",
                    hostname,
                    manufacturer_name,
                    model_name,
                    raw_hostname,
                )
            else:
                device.location = device.location or default_location
                device.role = device.role or device_role
                device.status = device_status
                device.device_type = device_type
                device.serial = serial or device.serial

                if platform is not None:
                    device.platform = platform

                if software_version is not None and self._model_has_field(Device, "software_version"):
                    device.software_version = software_version

                self._apply_custom_fields(device, custom_fields)
                device.validated_save()

                updated += 1
                self.logger.info(
                    "Updated device %s with DeviceType %s/%s from Automox hostname %s.",
                    device.name,
                    manufacturer_name,
                    model_name,
                    raw_hostname,
                )

        summary = (
            f"Automox sync complete: {created} created, {updated} updated, "
            f"{skipped} skipped, {skipped_vms} skipped as existing VMs."
        )
        self.logger.info(summary)
        return summary

    @staticmethod
    def _apply_custom_fields(device: Device, custom_fields: Dict[str, Any]) -> None:
        for key, value in custom_fields.items():
            device.cf[key] = value

    def _get_secret(self, secrets_group: SecretsGroup, secret_kind: str, required: bool = True) -> str:
        access_type = self._choice_value(
            SecretsGroupAccessTypeChoices,
            preferred_names=("TYPE_GENERIC", "TYPE_HTTP", "TYPE_REST"),
            fallback="generic",
        )

        if secret_kind == "username":
            secret_type = self._choice_value(
                SecretsGroupSecretTypeChoices,
                preferred_names=("TYPE_USERNAME",),
                fallback="username",
            )
        elif secret_kind == "password":
            secret_type = self._choice_value(
                SecretsGroupSecretTypeChoices,
                preferred_names=("TYPE_PASSWORD", "TYPE_TOKEN"),
                fallback="password",
            )
        elif secret_kind == "secret":
            secret_type = self._choice_value(
                SecretsGroupSecretTypeChoices,
                preferred_names=("TYPE_SECRET", "TYPE_TOKEN"),
                fallback="secret",
            )
        else:
            raise ValueError(f"Unsupported secret kind: {secret_kind}")

        try:
            value = secrets_group.get_secret_value(
                access_type=access_type,
                secret_type=secret_type,
            )
        except Exception as exc:
            if required:
                raise RuntimeError(
                    f"Could not retrieve {secret_kind!r} from Secrets Group {secrets_group!s}: {exc}"
                ) from exc
            return ""

        if required and not value:
            raise RuntimeError(
                f"Required secret {secret_kind!r} was empty in Secrets Group {secrets_group!s}."
            )

        return str(value or "").strip()

    @staticmethod
    def _choice_value(choice_class: Any, preferred_names: Iterable[str], fallback: str) -> str:
        for attr_name in preferred_names:
            if hasattr(choice_class, attr_name):
                return getattr(choice_class, attr_name)
        return fallback

    def _fetch_automox_servers(
        self,
        *,
        base_url: str,
        api_key: str,
        org_query_param: str,
        org_key: str,
        page_size: int,
    ) -> List[Dict[str, Any]]:
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        endpoint = f"{base_url.rstrip('/')}/servers"
        records: List[Dict[str, Any]] = []
        page = 0

        while True:
            params = {
                org_query_param: org_key,
                "limit": page_size,
                "page": page,
                "include_details": 1,
            }

            response = session.get(endpoint, params=params, timeout=60)

            if response.status_code in {400, 404} and page == 0:
                page = 1
                params["page"] = page
                response = session.get(endpoint, params=params, timeout=60)

            response.raise_for_status()
            payload = response.json()
            batch = self._extract_records(payload)

            if not batch:
                break

            records.extend(batch)

            if len(batch) < page_size:
                break

            if isinstance(payload, dict) and any(k in payload for k in ("next", "next_page")):
                if not payload.get("next") and not payload.get("next_page"):
                    break

            page += 1

            if page > 10000:
                raise RuntimeError(
                    "Aborting Automox pagination after 10,000 pages; check API response shape."
                )

        return records

    @staticmethod
    def _extract_records(payload: Any) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        if isinstance(payload, dict):
            for key in ("data", "results", "items", "servers"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]

        return []

    def _get_or_create_device_type(
        self,
        *,
        manufacturer_name: str,
        model_name: str,
        create_missing: bool,
    ) -> Optional[DeviceType]:
        manufacturer = Manufacturer.objects.filter(name__iexact=manufacturer_name).first()

        if manufacturer is None:
            if not create_missing:
                return None

            manufacturer_kwargs = {"name": manufacturer_name}

            if self._model_has_field(Manufacturer, "slug"):
                manufacturer_kwargs["slug"] = self._unique_slug(
                    Manufacturer,
                    manufacturer_name,
                )

            manufacturer = Manufacturer(**manufacturer_kwargs)
            manufacturer.validated_save()

        device_type = DeviceType.objects.filter(
            manufacturer=manufacturer,
            model__iexact=model_name,
        ).first()

        if device_type is not None:
            return device_type

        if not create_missing:
            return None

        device_type_kwargs = {
            "manufacturer": manufacturer,
            "model": model_name,
        }

        if self._model_has_field(DeviceType, "slug"):
            device_type_kwargs["slug"] = self._unique_slug(
                DeviceType,
                f"{manufacturer_name}-{model_name}",
            )

        device_type = DeviceType(**device_type_kwargs)
        device_type.validated_save()

        return device_type

    def _get_or_create_platform(self, record: Dict[str, Any]) -> Optional[Platform]:
        os_family = str(record.get("os_family") or "").strip()
        os_name = str(record.get("os_name") or "").strip()

        platform_name = " ".join(part for part in [os_family, os_name] if part).strip()

        if not platform_name:
            return None

        platform = Platform.objects.filter(name__iexact=platform_name).first()

        if platform is not None:
            return platform

        platform_kwargs = {"name": platform_name}

        if self._model_has_field(Platform, "slug"):
            platform_kwargs["slug"] = self._unique_slug(Platform, platform_name)

        platform = Platform(**platform_kwargs)
        platform.validated_save()

        return platform

    def _get_or_create_software_version(
        self,
        record: Dict[str, Any],
        *,
        platform: Optional[Platform],
    ) -> Optional[SoftwareVersion]:
        version = str(record.get("os_version") or "").strip()

        if not version:
            return None

        query = SoftwareVersion.objects.filter(version__iexact=version)

        if platform is not None and self._model_has_field(SoftwareVersion, "platform"):
            query = query.filter(platform=platform)

        software_version = query.first()

        if software_version is not None:
            return software_version

        software_version_kwargs = {"version": version}

        if platform is not None and self._model_has_field(SoftwareVersion, "platform"):
            software_version_kwargs["platform"] = platform

        if self._model_has_field(SoftwareVersion, "alias"):
            software_version_kwargs["alias"] = version

        software_version = SoftwareVersion(**software_version_kwargs)
        software_version.validated_save()

        return software_version

    @staticmethod
    def _model_has_field(model: Any, field_name: str) -> bool:
        return any(field.name == field_name for field in model._meta.get_fields())

    @staticmethod
    def _custom_fields_from_automox(record: Dict[str, Any], *, set_rmm_field: bool) -> Dict[str, Any]:
        custom_fields: Dict[str, Any] = {
            "agent_version": SyncAutomoxDevices._first_string(record, "agent_version") or "",
            "cpu": SyncAutomoxDevices._cpu_value(record),
            "installed_ram": SyncAutomoxDevices._ram_value(record),
            "needs_attention": SyncAutomoxDevices._boolish_string(record.get("needs_attention")),
            "needs_reboot": SyncAutomoxDevices._boolish_string(record.get("needs_reboot")),
            "pending_patches": SyncAutomoxDevices._pending_patch_count(record),
            "last_network_data_sync": date.today().isoformat(),
        }

        if set_rmm_field:
            custom_fields["rmm"] = "Automox"

        return custom_fields

    @staticmethod
    def _find_device_by_hostname(
        record: Dict[str, Any],
        raw_hostname: str,
        normalized_hostname: str,
    ) -> Optional[Device]:
        for candidate in SyncAutomoxDevices._hostname_candidates(record, raw_hostname, normalized_hostname):
            device = Device.objects.filter(name__iexact=candidate).first()
            if device is not None:
                return device
        return None

    @staticmethod
    def _find_virtual_machine_by_hostname(
        record: Dict[str, Any],
        raw_hostname: str,
        normalized_hostname: str,
    ) -> Optional[VirtualMachine]:
        for candidate in SyncAutomoxDevices._hostname_candidates(record, raw_hostname, normalized_hostname):
            vm = VirtualMachine.objects.filter(name__iexact=candidate).first()
            if vm is not None:
                return vm
        return None

    @staticmethod
    def _hostname_candidates(
        record: Dict[str, Any],
        raw_hostname: str,
        normalized_hostname: str,
    ) -> List[str]:
        candidates = []

        for value in (
            raw_hostname,
            normalized_hostname,
            record.get("name"),
            record.get("display_name"),
            record.get("custom_name"),
        ):
            if value:
                candidates.append(str(value).strip())
                candidates.append(SyncAutomoxDevices._normalize_hostname(str(value)))

        detail = SyncAutomoxDevices._detail(record)
        fqdns = SyncAutomoxDevices._detail_value(detail, "FQDNS")

        if isinstance(fqdns, list):
            for fqdn in fqdns:
                if fqdn:
                    candidates.append(str(fqdn).strip())
                    candidates.append(SyncAutomoxDevices._normalize_hostname(str(fqdn)))

        return [candidate for candidate in dict.fromkeys(candidates) if candidate]

    @staticmethod
    def _hostname(record: Dict[str, Any]) -> str:
        value = SyncAutomoxDevices._first_string(
            record,
            "name",
            "display_name",
            "hostname",
            "server_name",
            "fqdn",
        )

        if value:
            return value.strip()[:255]

        detail = SyncAutomoxDevices._detail(record)
        fqdns = SyncAutomoxDevices._detail_value(detail, "FQDNS")

        if isinstance(fqdns, list) and fqdns:
            return str(fqdns[0]).strip()[:255]

        return ""

    @staticmethod
    def _normalize_hostname(value: str) -> str:
        if not value:
            return ""

        value = value.strip().lower().rstrip(".")
        short_name = value.split(".", 1)[0]

        return short_name[:64]

    @staticmethod
    def _manufacturer_name(record: Dict[str, Any]) -> str:
        detail = SyncAutomoxDevices._detail(record)
        return str(SyncAutomoxDevices._detail_value(detail, "VENDOR") or "Unknown").strip()

    @staticmethod
    def _model_name(record: Dict[str, Any]) -> str:
        detail = SyncAutomoxDevices._detail(record)
        return str(SyncAutomoxDevices._detail_value(detail, "MODEL") or "Unknown").strip()

    @staticmethod
    def _serial_number(record: Dict[str, Any]) -> str:
        detail = SyncAutomoxDevices._detail(record)
        return str(
            SyncAutomoxDevices._detail_value(detail, "SERIAL")
            or SyncAutomoxDevices._detail_value(detail, "SERVICETAG")
            or record.get("serial_number")
            or ""
        ).strip()

    @staticmethod
    def _cpu_value(record: Dict[str, Any]) -> str:
        detail = SyncAutomoxDevices._detail(record)
        return str(SyncAutomoxDevices._detail_value(detail, "CPU") or "").strip()

    @staticmethod
    def _ram_value(record: Dict[str, Any]) -> str:
        detail = SyncAutomoxDevices._detail(record)
        ram = SyncAutomoxDevices._detail_value(detail, "RAM")
        return SyncAutomoxDevices._format_ram_value(ram)

    @staticmethod
    def _format_ram_value(value: Any) -> str:
        if value is None or value == "":
            return ""

        text = str(value).strip()

        if not text.isdigit():
            return text

        try:
            bytes_value = int(text)
            gb_value = bytes_value / (1024 ** 3)

            if gb_value.is_integer():
                return f"{int(gb_value)} GB"

            return f"{gb_value:.2f} GB"

        except Exception:
            return text

    @staticmethod
    def _detail(record: Dict[str, Any]) -> Dict[str, Any]:
        detail = record.get("detail") or {}

        if isinstance(detail, dict):
            return detail

        if isinstance(detail, str):
            try:
                parsed = json.loads(detail)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                return {}

        return {}

    @staticmethod
    def _detail_value(detail: Dict[str, Any], key: str) -> Any:
        if key in detail:
            return detail[key]

        key_lower = key.lower()
        for existing_key, value in detail.items():
            if str(existing_key).lower() == key_lower:
                return value

        return None

    @staticmethod
    def _first_string(record: Dict[str, Any], *keys: str) -> str:
        for key in keys:
            value = record.get(key)
            if value is not None and value != "":
                return str(value).strip()
        return ""

    @staticmethod
    def _pending_patch_count(record: Dict[str, Any]) -> str:
        for key in ("pending_patches", "patches", "patch_count", "pending_patch_count"):
            value = record.get(key)
            if isinstance(value, list):
                return str(len(value))
            if value is not None and value != "":
                return str(value)
        return ""

    @staticmethod
    def _boolish_string(value: Any) -> str:
        if value is True:
            return "True"
        if value is False:
            return "False"
        if value is None:
            return ""
        return str(value)

    @staticmethod
    def _safe_record_id(record: Dict[str, Any]) -> str:
        return str(
            record.get("id")
            or record.get("uuid")
            or record.get("server_id")
            or "unknown"
        )

    @staticmethod
    def _unique_slug(model: Any, value: str) -> str:
        base = slugify(value) or "unknown"
        base = re.sub(r"[^a-z0-9_-]+", "-", base.lower()).strip("-") or "unknown"

        slug = base[:50]
        counter = 1

        while model.objects.filter(slug=slug).exists():
            suffix = f"-{counter}"
            slug = f"{base[:50 - len(suffix)]}{suffix}"
            counter += 1

        return slug


register_jobs(SyncAutomoxDevices)
