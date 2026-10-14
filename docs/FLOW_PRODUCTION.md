# Flow production configuration

Flow uses separate credentials for its production and Sandbox accounts. Never commit credentials or paste them into chat.

To enable real payments after adding the production API key and Secret Key in the hosting environment, configure:

- `FLOW_ENV=production`
- `FLOW_API_URL=https://www.flow.cl/api`
- `FLOW_PRODUCTION_API_KEY=<production API key>`
- `FLOW_PRODUCTION_SECRET_KEY=<production Secret Key>`
- `GESTIONSTOCK_FLOW_ENABLED=1`

If Sandbox payments must remain queryable for older pending transactions, also configure `FLOW_SANDBOX_API_KEY` and `FLOW_SANDBOX_SECRET_KEY` with the Sandbox account credentials. New transactions use the environment selected by `FLOW_ENV`; each saved transaction retains its environment for later status checks.

Before enabling checkout, verify `GESTIONSTOCK_FLOW_FEE_RATE_PCT`, `GESTIONSTOCK_FLOW_FEE_IVA_PCT`, and `GESTIONSTOCK_FLOW_FEE_FIXED_CLP` against Sucree's current production agreement. The application must not assume Sandbox pricing is the production fee.

To return to test mode, set `FLOW_ENV=sandbox`, `FLOW_API_URL=https://sandbox.flow.cl/api`, and either `FLOW_SANDBOX_API_KEY` / `FLOW_SANDBOX_SECRET_KEY` or the legacy generic `FLOW_API_KEY` / `FLOW_SECRET_KEY` to Sandbox credentials. Production activation deliberately ignores the generic keys, so Sandbox credentials cannot accidentally enable real charges. Flow links are accepted only for HTTPS hosts belonging to the selected environment.

After changing these server variables, restart the web application. Production credentials are not part of the repository or APK.
