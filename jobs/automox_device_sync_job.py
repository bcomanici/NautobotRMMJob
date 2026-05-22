"""
Nautobot Job: Sync Automox devices into Nautobot Devices.

Drop this file into your Nautobot JOBS_ROOT, for example:
    $NAUTOBOT_ROOT/jobs/automox_device_sync_job.py

Expected Nautobot Secrets Group entries:
    Access Type: Generic, Secret Type: Username  -> Automox username/account label (optional for API calls)
    Access Type: Generic, Secret Type: Password  -> Automox API key/token
    Access Type: Generic, Secret Type: Secret    -> Automox organization ID/key used as the `o` query parameter

This job uses the existing Nautobot device custom fields from the provided export:
    rmm
    agent_version
    cpu
    installed_ram
    needs_attention
    needs_reboot
    pending_patches
    last_network_data_sync
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, Iterable, List, Optional

import requests
from django.contrib.contenttypes.models import ContentType
from django.utils.text import slugify

from nautobot.apps.jobs import BooleanVar, IntegerVar, Job, ObjectVar, StringVar, register_jobs
from nautobot.dcim.models import Device, DeviceType, Location, Manufacturer
from nautobot.extras.choices import SecretsGroupAccessTypeChoices, SecretsGroupSecretTypeChoices
from nautobot.extras.models import Role, SecretsGroup, Status

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
        description="Create missing Manufacturer and DeviceType records from Automox make/model data.",
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

        # Username is intentionally not used by Automox API-key authentication, but retrieving it validates that
        # the selected Secrets Group contains the complete credential bundle the team expects.
        if automox_username:
            self.logger.info("Using Automox Secrets Group credential set for user/account: %s", automox_username)

        devices = self._fetch_automox_servers(
            base_url=automox_base_url,
            api_key=automox_api_key,
            org_query_param=automox_org_query_param,
            org_key=automox_org_key,
            page_size=request_page_size,
        )

        self.logger.info("Fetched %s Automox device records.", len(devices))

        created = 0
        updated = 0
        skipped = 0

        for record in devices:
            hostname = self._hostname(record)
            if not hostname:
                skipped += 1
                self.logger.warning("Skipping Automox record without a usable hostname/name: %s", self._safe_record_id(record))
                continue

            device = Device.objects.filter(name=hostname).first()
            if device and not update_existing_devices:
                skipped += 1
                self.logger.info("Skipping existing device because updates are disabled: %s", hostname)
                continue

            manufacturer_name = self._first_string(record, "make", "manufacturer", "vendor") or "Unknown"
            model_name = self._first_string(record, "model", "model_name", "hardware_model") or "Unknown"
            serial = self._first_string(record, "serial_number", "serial", "serialnum", "service_tag") or ""

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

            custom_fields = self._custom_fields_from_automox(record, set_rmm_field=set_rmm_field)

            if device is None:
                device = Device(
                    name=hostname,
                    location=default_location,
                    role=device_role,
                    status=device_status,
                    device_type=device_type,
                    serial=serial,
                )
                device.custom_field_data = custom_fields
                device.validated_save()
                created += 1
                self.logger.info("Created device %s.", hostname)
            else:
                device.location = device.location or default_location
                device.role = device.role or device_role
                device.status = device_status
                device.device_type = device_type
                device.serial = serial or device.serial
                device.custom_field_data = {
                    **(device.custom_field_data or {}),
                    **custom_fields,
                }
                device.validated_save()
                updated += 1
                self.logger.info("Updated device %s.", hostname)

        summary = f"Automox sync complete: {created} created, {updated} updated, {skipped} skipped."
        self.logger.info(summary)
        return summary

    def _get_secret(self, secrets_group: SecretsGroup, secret_kind: str, required: bool = True) -> str:
        """Read a value from the selected Secrets Group using Generic access type."""
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
            value = secrets_group.get_secret_value(access_type=access_type, secret_type=secret_type)
        except Exception as exc:  # noqa: BLE001 - surface secret lookup failures as clean Job errors
            if required:
                raise RuntimeError(
                    f"Could not retrieve {secret_kind!r} from Secrets Group {secrets_group!s} "
                    f"using access_type={access_type!r}, secret_type={secret_type!r}: {exc}"
                ) from exc
            return ""

        if required and not value:
            raise RuntimeError(f"Required secret {secret_kind!r} was empty in Secrets Group {secrets_group!s}.")
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
        """Fetch Automox server/device records, handling common list and paginated response shapes."""
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
            }
            response = session.get(endpoint, params=params, timeout=60)

            # Some APIs use 1-based pages. If page 0 is rejected, retry once at page 1.
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

            # Stop if Automox returns an explicit next link/key and it is empty/false.
            if isinstance(payload, dict) and any(k in payload for k in ("next", "next_page")):
                if not payload.get("next") and not payload.get("next_page"):
                    break

            page += 1
            if page > 10000:
                raise RuntimeError("Aborting Automox pagination after 10,000 pages; check API response shape.")

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
        manufacturer = Manufacturer.objects.filter(name=manufacturer_name).first()
        if manufacturer is None:
            if not create_missing:
                return None
            manufacturer = Manufacturer(name=manufacturer_name, slug=self._unique_slug(Manufacturer, manufacturer_name))
            manufacturer.validated_save()

        device_type = DeviceType.objects.filter(manufacturer=manufacturer, model=model_name).first()
        if device_type is not None:
            return device_type

        if not create_missing:
            return None

        device_type = DeviceType(
            manufacturer=manufacturer,
            model=model_name,
            slug=self._unique_slug(DeviceType, f"{manufacturer_name}-{model_name}"),
        )
        device_type.validated_save()
        return device_type

    @staticmethod
    def _custom_fields_from_automox(record: Dict[str, Any], *, set_rmm_field: bool) -> Dict[str, Any]:
        custom_fields: Dict[str, Any] = {
            "agent_version": SyncAutomoxDevices._first_string(record, "agent_version", "agentVersion") or "",
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
    def _hostname(record: Dict[str, Any]) -> str:
        value = SyncAutomoxDevices._first_string(record, "name", "hostname", "display_name", "server_name")
        if not value:
            return ""
        # Nautobot Device.name max length is generally 64 chars in many deployments.
        return value.strip()[:64]

    @staticmethod
    def _first_string(record: Dict[str, Any], *keys: str) -> str:
        for key in keys:
            value = record.get(key)
            if value is not None and value != "":
                return str(value).strip()
        return ""

    @staticmethod
    def _cpu_value(record: Dict[str, Any]) -> str:
        for key in ("cpu", "processor", "processors", "processor_model", "cpu_model"):
            value = record.get(key)
            if value:
                return str(value)
        details = record.get("detail") or record.get("details") or {}
        if isinstance(details, dict):
            return str(details.get("cpu") or details.get("processor") or "")
        return ""

    @staticmethod
    def _ram_value(record: Dict[str, Any]) -> str:
        for key in ("ram", "total_memory", "memory", "memory_size", "installed_ram"):
            value = record.get(key)
            if value:
                return str(value)
        details = record.get("detail") or record.get("details") or {}
        if isinstance(details, dict):
            return str(details.get("ram") or details.get("total_memory") or details.get("memory") or "")
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
        return str(record.get("id") or record.get("uuid") or record.get("server_id") or "unknown")

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
