"""Configuratieflow voor Storm Tracker V3."""
from __future__ import annotations

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.helpers import selector

from .const import (
    CONF_NOTIFY_SERVICE,
    CONF_RUNTIME_MODE,
    CONF_WARNING_NOTIFICATIONS_ENABLED,
    DOMAIN,
    RUNTIME_MODE_LEGACY,
    RUNTIME_MODE_WORKER_ONLY,
)

CONF_PERSONS = "persons"
CONF_TEST_TRACKER = "test_tracker_entity"
CONF_RADAR_RADIUS = "radar_radius_km"
CONF_SHARING_DISTANCE = "engine_sharing_distance_km"
CONF_EUMETSAT_KEY = "eumetsat_consumer_key"
CONF_EUMETSAT_SECRET = "eumetsat_consumer_secret"
CONF_METEOFRANCE_TOKEN = "meteofrance_api_token"
CONF_METEOFRANCE_APPLICATION_ID = "meteofrance_application_id"
CONF_KNMI_API_KEY = "knmi_api_key"
CONF_KNMI_WMS_API_KEY = "knmi_wms_api_key"
CONF_HSAF_USERNAME = "hsaf_username"
CONF_HSAF_PASSWORD = "hsaf_password"
CONF_LIGHTNING_SOURCE_MODE = "lightning_source_mode"
CONF_NETATMO_CLIENT_ID = "netatmo_client_id"
CONF_NETATMO_CLIENT_SECRET = "netatmo_client_secret"
CONF_NETATMO_REFRESH_TOKEN = "netatmo_refresh_token"
CONF_NETATMO_RADIUS = "netatmo_radius_km"
CONF_OPEN_METEO_ENABLED = "open_meteo_enabled"
CONF_ANALYSIS_MODE = "analysis_mode"


PROVIDER_CREDENTIAL_FIELDS = frozenset({
    CONF_EUMETSAT_KEY,
    CONF_EUMETSAT_SECRET,
    CONF_METEOFRANCE_TOKEN,
    CONF_METEOFRANCE_APPLICATION_ID,
    CONF_KNMI_API_KEY,
    CONF_KNMI_WMS_API_KEY,
    CONF_HSAF_USERNAME,
    CONF_HSAF_PASSWORD,
    CONF_NETATMO_CLIENT_ID,
    CONF_NETATMO_CLIENT_SECRET,
    CONF_NETATMO_REFRESH_TOKEN,
})
WORKER_ONLY_SCHEMA_FIELDS = frozenset({
    CONF_PERSONS,
    CONF_TEST_TRACKER,
    CONF_RADAR_RADIUS,
    CONF_SHARING_DISTANCE,
    CONF_NETATMO_RADIUS,
    CONF_LIGHTNING_SOURCE_MODE,
    CONF_OPEN_METEO_ENABLED,
})
LEGACY_SCHEMA_FIELDS = frozenset({
    *WORKER_ONLY_SCHEMA_FIELDS,
    *PROVIDER_CREDENTIAL_FIELDS,
    CONF_ANALYSIS_MODE,
    CONF_NOTIFY_SERVICE,
    CONF_WARNING_NOTIFICATIONS_ENABLED,
})
RUNTIME_MODE_OPTIONS = (
    RUNTIME_MODE_WORKER_ONLY,
    RUNTIME_MODE_LEGACY,
)


def _runtime_mode_schema(default: str) -> vol.Schema:
    """Require one visible, exact runtime choice; worker-only is the default."""

    return vol.Schema({
        vol.Required(CONF_RUNTIME_MODE, default=default):
            selector.SelectSelector(selector.SelectSelectorConfig(
                options=list(RUNTIME_MODE_OPTIONS),
                mode=selector.SelectSelectorMode.DROPDOWN,
                translation_key="runtime_mode",
            )),
    })


def _base_fields(defaults: dict) -> dict:
    """Build only the non-secret worker-owned desired-state fields."""

    return {
        vol.Optional(CONF_PERSONS, default=defaults.get(CONF_PERSONS, [])):
            selector.EntitySelector(selector.EntitySelectorConfig(
                domain="device_tracker", integration="life360", multiple=True,
            )),
        vol.Optional(CONF_RADAR_RADIUS, default=defaults.get(CONF_RADAR_RADIUS, 300.0)):
            selector.NumberSelector(selector.NumberSelectorConfig(
                min=50, max=1000, step=10, mode=selector.NumberSelectorMode.BOX,
                unit_of_measurement="km",
            )),
        vol.Optional(
            CONF_SHARING_DISTANCE,
            default=defaults.get(CONF_SHARING_DISTANCE, 150.0),
        ): selector.NumberSelector(selector.NumberSelectorConfig(
            min=25, max=500, step=25, mode=selector.NumberSelectorMode.BOX,
            unit_of_measurement="km",
        )),
        vol.Optional(
            CONF_LIGHTNING_SOURCE_MODE,
            default=defaults.get(CONF_LIGHTNING_SOURCE_MODE, "auto"),
        ): selector.SelectSelector(selector.SelectSelectorConfig(
            options=["auto", "satellite_test"],
            mode=selector.SelectSelectorMode.DROPDOWN,
            translation_key="lightning_source_mode",
        )),
        vol.Optional(
            CONF_OPEN_METEO_ENABLED,
            default=defaults.get(CONF_OPEN_METEO_ENABLED, False),
        ): selector.BooleanSelector(),
    }


def _add_test_tracker(fields: dict, defaults: dict) -> None:
    test_selector = selector.EntitySelector(selector.EntitySelectorConfig(
        domain="device_tracker",
    ))
    if defaults.get(CONF_TEST_TRACKER):
        fields[vol.Optional(
            CONF_TEST_TRACKER, default=defaults[CONF_TEST_TRACKER]
        )] = test_selector
    else:
        fields[vol.Optional(CONF_TEST_TRACKER)] = test_selector


def _worker_only_schema(defaults: dict) -> vol.Schema:
    """Return the HA-owned, provider-credential-free worker-only schema."""

    fields = _base_fields(defaults)
    fields[vol.Optional(
        CONF_NETATMO_RADIUS,
        default=min(
            float(defaults.get(CONF_NETATMO_RADIUS, 20.0)), 20.0
        ),
    )] = selector.NumberSelector(selector.NumberSelectorConfig(
        min=10, max=20, step=5, mode=selector.NumberSelectorMode.BOX,
        unit_of_measurement="km",
    ))
    _add_test_tracker(fields, defaults)
    return vol.Schema(fields)


def _schema(defaults: dict) -> vol.Schema:
    """Return the unchanged legacy schema for old-entry compatibility."""

    fields = _base_fields(defaults)
    fields.update({
        vol.Optional(
            CONF_EUMETSAT_KEY,
            default=defaults.get(CONF_EUMETSAT_KEY, ""),
        ): selector.TextSelector(),
        vol.Optional(
            CONF_EUMETSAT_SECRET,
            default=defaults.get(CONF_EUMETSAT_SECRET, ""),
        ): selector.TextSelector(selector.TextSelectorConfig(
            type=selector.TextSelectorType.PASSWORD,
        )),
        vol.Optional(
            CONF_METEOFRANCE_TOKEN,
            default=defaults.get(CONF_METEOFRANCE_TOKEN, ""),
        ): selector.TextSelector(selector.TextSelectorConfig(
            type=selector.TextSelectorType.PASSWORD,
        )),
        vol.Optional(
            CONF_METEOFRANCE_APPLICATION_ID,
            default=defaults.get(CONF_METEOFRANCE_APPLICATION_ID, ""),
        ): selector.TextSelector(selector.TextSelectorConfig(
            type=selector.TextSelectorType.PASSWORD,
        )),
        vol.Optional(
            CONF_KNMI_API_KEY,
            default=defaults.get(CONF_KNMI_API_KEY, ""),
        ): selector.TextSelector(selector.TextSelectorConfig(
            type=selector.TextSelectorType.PASSWORD,
        )),
        vol.Optional(
            CONF_KNMI_WMS_API_KEY,
            default=defaults.get(CONF_KNMI_WMS_API_KEY, ""),
        ): selector.TextSelector(selector.TextSelectorConfig(
            type=selector.TextSelectorType.PASSWORD,
        )),
        vol.Optional(
            CONF_HSAF_USERNAME,
            default=defaults.get(CONF_HSAF_USERNAME, ""),
        ): selector.TextSelector(),
        vol.Optional(
            CONF_HSAF_PASSWORD,
            default=defaults.get(CONF_HSAF_PASSWORD, ""),
        ): selector.TextSelector(selector.TextSelectorConfig(
            type=selector.TextSelectorType.PASSWORD,
        )),
        vol.Optional(
            CONF_NETATMO_CLIENT_ID,
            default=defaults.get(CONF_NETATMO_CLIENT_ID, ""),
        ): selector.TextSelector(),
        vol.Optional(
            CONF_NETATMO_CLIENT_SECRET,
            default=defaults.get(CONF_NETATMO_CLIENT_SECRET, ""),
        ): selector.TextSelector(selector.TextSelectorConfig(
            type=selector.TextSelectorType.PASSWORD,
        )),
        vol.Optional(
            CONF_NETATMO_REFRESH_TOKEN,
            default=defaults.get(CONF_NETATMO_REFRESH_TOKEN, ""),
        ): selector.TextSelector(selector.TextSelectorConfig(
            type=selector.TextSelectorType.PASSWORD,
        )),
        vol.Optional(
            CONF_NETATMO_RADIUS,
            default=defaults.get(CONF_NETATMO_RADIUS, 175.0),
        ): selector.NumberSelector(selector.NumberSelectorConfig(
            min=25, max=500, step=25,
            mode=selector.NumberSelectorMode.BOX,
            unit_of_measurement="km",
        )),
        vol.Optional(
            CONF_ANALYSIS_MODE,
            default=defaults.get(CONF_ANALYSIS_MODE, "full_send"),
        ): selector.SelectSelector(selector.SelectSelectorConfig(
            options=["full_send", "thermal_throttle"],
            mode=selector.SelectSelectorMode.DROPDOWN,
            translation_key="analysis_mode",
        )),
        vol.Optional(
            CONF_WARNING_NOTIFICATIONS_ENABLED,
            default=defaults.get(
                CONF_WARNING_NOTIFICATIONS_ENABLED, False
            ),
        ): selector.BooleanSelector(),
        vol.Optional(
            CONF_NOTIFY_SERVICE,
            default=defaults.get(CONF_NOTIFY_SERVICE, ""),
        ): selector.TextSelector(),
    })
    _add_test_tracker(fields, defaults)
    return vol.Schema(fields)


def _worker_only_input(user_input: dict) -> dict:
    """Copy only fields HA is allowed to own in worker-only mode."""

    return {
        key: value
        for key, value in user_input.items()
        if key in WORKER_ONLY_SCHEMA_FIELDS
    }


def _legacy_input(user_input: dict) -> dict:
    """Copy only fields owned by the explicit normal-HA runtime."""

    return {
        key: value
        for key, value in user_input.items()
        if key in LEGACY_SCHEMA_FIELDS
    }


def _worker_only_entry_values(config_entry) -> dict:
    """Read only allowlisted HA-owned values from an existing entry."""

    projected = {
        key: config_entry.data[key]
        for key in WORKER_ONLY_SCHEMA_FIELDS
        if key in config_entry.data
    }
    projected.update({
        key: config_entry.options[key]
        for key in WORKER_ONLY_SCHEMA_FIELDS
        if key in config_entry.options
    })
    return projected


def _legacy_entry_values(config_entry) -> dict:
    """Read only the explicit normal-HA allowlist from an existing entry."""

    projected = {
        key: config_entry.data[key]
        for key in LEGACY_SCHEMA_FIELDS
        if key in config_entry.data
    }
    projected.update({
        key: config_entry.options[key]
        for key in LEGACY_SCHEMA_FIELDS
        if key in config_entry.options
    })
    return projected


def _entry_mode(config_entry) -> str | None:
    options_mode = config_entry.options.get(CONF_RUNTIME_MODE)
    data_mode = config_entry.data.get(CONF_RUNTIME_MODE)
    if options_mode not in (None, data_mode):
        return "invalid"
    return data_mode


def _preserve_hidden_options(config_entry, visible_options: dict) -> dict:
    """Preserve stored options exactly and overlay only allowlisted input.

    The worker-only form must not project legacy credentials, yet absence from
    a submitted form is not authority to delete an already stored value.  This
    also preserves explicit false values (notably ``open_meteo_enabled``)
    instead of silently replacing durable state with an implicit default.
    ``runtime_mode`` is data-owned in schema v3 and is the sole excluded key.
    """

    preserved = {
        key: value
        for key, value in config_entry.options.items()
        if key != CONF_RUNTIME_MODE
    }
    preserved.update(visible_options)
    return preserved


class StormTrackerConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Maak precies een Storm Tracker-installatie aan."""

    VERSION = 3

    async def async_step_user(self, user_input=None):
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")
        if user_input is not None:
            mode = user_input.get(CONF_RUNTIME_MODE)
            if mode not in RUNTIME_MODE_OPTIONS:
                return self.async_abort(reason="invalid_runtime_mode")
            self._pending_create_mode = mode
            if mode == RUNTIME_MODE_LEGACY:
                return await self.async_step_legacy()
            return await self.async_step_worker_only()
        return self.async_show_form(
            step_id="user",
            data_schema=_runtime_mode_schema(RUNTIME_MODE_WORKER_ONLY),
        )

    async def async_step_worker_only(self, user_input=None):
        """Create a secret-free worker-only entry after an explicit choice."""

        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")
        if getattr(self, "_pending_create_mode", None) != RUNTIME_MODE_WORKER_ONLY:
            return self.async_abort(reason="worker_only_mode_required")
        if user_input is not None:
            data = _worker_only_input(dict(user_input))
            data[CONF_RUNTIME_MODE] = RUNTIME_MODE_WORKER_ONLY
            return self.async_create_entry(title="Storm Tracker V3", data=data)
        return self.async_show_form(
            step_id="worker_only", data_schema=_worker_only_schema({})
        )

    async def async_step_legacy(self, user_input=None):
        """Create a normal-HA entry; credentials exist only on this form."""

        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")
        if getattr(self, "_pending_create_mode", None) != RUNTIME_MODE_LEGACY:
            return self.async_abort(reason="legacy_mode_required")
        if user_input is not None:
            data = _legacy_input(dict(user_input))
            data[CONF_RUNTIME_MODE] = RUNTIME_MODE_LEGACY
            return self.async_create_entry(title="Storm Tracker V3", data=data)
        return self.async_show_form(
            step_id="legacy", data_schema=_schema({})
        )

    async def async_step_reconfigure(self, user_input=None):
        """Switch an existing entry between two exact, persisted modes."""

        entry = self._get_reconfigure_entry()
        current_mode = _entry_mode(entry)
        if current_mode not in RUNTIME_MODE_OPTIONS:
            return self.async_abort(reason="invalid_runtime_mode")
        if user_input is not None:
            mode = user_input.get(CONF_RUNTIME_MODE)
            if mode not in RUNTIME_MODE_OPTIONS:
                return self.async_abort(reason="invalid_runtime_mode")
            self._pending_reconfigure_mode = mode
            if mode == RUNTIME_MODE_LEGACY:
                return await self.async_step_reconfigure_legacy()
            return await self.async_step_reconfigure_worker_only()
        return self.async_show_form(
            step_id="reconfigure",
            data_schema=_runtime_mode_schema(current_mode),
        )

    async def async_step_reconfigure_worker_only(self, user_input=None):
        """Replace the entry with a closed, credential-free worker payload."""

        if (
            getattr(self, "_pending_reconfigure_mode", None)
            != RUNTIME_MODE_WORKER_ONLY
        ):
            return self.async_abort(reason="worker_only_mode_required")
        entry = self._get_reconfigure_entry()
        if user_input is not None:
            data = _worker_only_input(dict(user_input))
            data[CONF_RUNTIME_MODE] = RUNTIME_MODE_WORKER_ONLY
            return self.async_update_and_abort(
                entry,
                data=data,
                options={},
            )
        return self.async_show_form(
            step_id="reconfigure_worker_only",
            data_schema=_worker_only_schema(
                _worker_only_entry_values(entry)
            ),
        )

    async def async_step_reconfigure_legacy(self, user_input=None):
        """Replace the entry with a closed normal-HA provider payload."""

        if (
            getattr(self, "_pending_reconfigure_mode", None)
            != RUNTIME_MODE_LEGACY
        ):
            return self.async_abort(reason="legacy_mode_required")
        entry = self._get_reconfigure_entry()
        if user_input is not None:
            data = _legacy_input(dict(user_input))
            data[CONF_RUNTIME_MODE] = RUNTIME_MODE_LEGACY
            return self.async_update_and_abort(
                entry,
                data=data,
                options={},
            )
        return self.async_show_form(
            step_id="reconfigure_legacy",
            data_schema=_schema(_legacy_entry_values(entry)),
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return StormTrackerOptionsFlow()


class StormTrackerOptionsFlow(config_entries.OptionsFlow):
    """Beheer Life360-personen en de optionele testtracker."""

    async def async_step_init(self, user_input=None):
        mode = _entry_mode(self.config_entry)
        if mode == RUNTIME_MODE_LEGACY:
            return await self.async_step_legacy(user_input)
        if mode != RUNTIME_MODE_WORKER_ONLY:
            return self.async_abort(reason="invalid_runtime_mode")
        if user_input is not None:
            options = dict(user_input)
            options.pop(CONF_RUNTIME_MODE, None)
            options = _preserve_hidden_options(
                self.config_entry,
                _worker_only_input(options),
            )
            return self.async_create_entry(title="", data=options)
        defaults = _worker_only_entry_values(self.config_entry)
        return self.async_show_form(
            step_id="init",
            data_schema=_worker_only_schema(defaults),
        )

    async def async_step_legacy(self, user_input=None):
        """Expose provider credentials only to an explicit legacy owner."""

        if _entry_mode(self.config_entry) != RUNTIME_MODE_LEGACY:
            return self.async_abort(reason="legacy_mode_required")

        if user_input is not None:
            options = dict(user_input)
            options.pop(CONF_RUNTIME_MODE, None)
            return self.async_create_entry(title="", data=options)
        defaults = {**self.config_entry.data, **self.config_entry.options}
        return self.async_show_form(
            step_id="legacy",
            data_schema=_schema(defaults),
        )
