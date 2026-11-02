# Privacy implementation: verified technical inventory

Updated: 2026-11-02. This is a code-level inventory, not legal advice or a complete production/provider audit.

## Observed data flows

| Flow | Observed fields/storage | Observed purpose | Current controls / unresolved items |
|---|---|---|---|
| Online shop checkout | Name, email, telephone, items/variants, totals, coupon, pickup/delivery type, delivery address and optional map coordinates; persisted with online sale records and customer accounts | Receive, prepare, charge and deliver orders; account history | Context notice added. Confirm exact required fields, payment provider terms, accounting retention, deletion exceptions and access roles. |
| Cake booking | Name, email, telephone, date/time, order details, pickup/delivery, address and coordinates; `agenda_eventos` | Receive and manage reservations | Context notice added. Free-text notes can contain unnecessary/sensitive data; retention and delivery-provider roles/locations need review. |
| Customer account | Name, email, phone, optional birth date/default address/location, account status, loyalty points and coupon assignments; `tienda_clientes` and related tables | Sign-in, order history and benefits | Account creation requires an affirmative privacy/account notice acceptance. `privacy_account_consents` records customer id, accepted notice/terms versions, scope, timestamp and separate optional marketing channel choices. The API rejects registration without acceptance. |
| Welcome/QR promotion | Email, phone, customer/coupon references, generated code, expiry and required consent for account/promo data use; `redes_promo_codes`, coupon tables and `privacy_account_consents` | Create the account and associate the requested one-use benefit | Separate, unchecked email/WhatsApp marketing choices are recorded per channel. Consent evidence also records the required account notice. Verify operational unsubscribe integration before campaigns. |
| Store live analytics | `tienda_visitas` and `tienda_visitas_eventos` can contain session id, page, cart contents/totals, user agent, IP and registered customer email/phone | Internal visit/cart/checkout metrics | New events are gated by explicit `analytics_internal` preference; no new browser session id is created when denied. Historical rows remain untouched pending approved retention/deletion rules. |
| Networks and QR metrics | `redes_events` / `redes_qr_events`: event, QR/link id, source/referrer host, campaign and timestamp | Measure page, link and QR usage | New events are gated by explicit `analytics_internal` preference. Existing metrics remain untouched. Counts will exclude visitors who do not opt in. |
| Assistant/contact | Free text can include customer contact, order, preferences and other details; source is the public assistant endpoint | Answer questions and prepare customer requests | Verify model/service provider, prompts/logging, subprocessors, data location and retention. Tell customers not to enter sensitive information until the flow is fully mapped. |
| Admin and security | Admin usernames, password hashes, login timestamps/IP in existing audit tables, roles/sessions and backups | Authenticate administrators, prevent abuse and restore operations | Existing auth is reused. MFA and fine-grained privacy-role authorization were not present in the reviewed app; require operational hardening. Set the production session secure flag and rotate secrets. |

## Third-party requests found in templates

- Google Fonts (`fonts.googleapis.com` / font delivery) is imported by several templates.
- jsPDF/jsPDF AutoTable are loaded from cdnjs in shop and agenda pages.
- Leaflet assets are loaded from unpkg. Map tiles, geocoding and address searches are requested from OpenStreetMap tile/Nominatim services.
- WhatsApp links transfer the user to WhatsApp when clicked; Flow is used by payment integration.

The code does not establish the actual processing country, contract role, subprocessors, transfer mechanism or current vendor terms. Those fields are intentionally left for verified configuration and legal review. Map/address searches can disclose the entered address to the map service; an inline notice and manual-address alternative are shown.

## Retention and publication

The admin retention tool stores a proposed number of days and justification per dataset and can count candidate rows only. It never deletes/changes operational or historical data. No policy is publicly labelled as approved until all required fields are filled, contact details/date are valid and an authorized administrator confirms legal review.

The admin editor now starts with a code-derived draft of known fields; unknown legal identity, vendor-country, retention and compliance details are explicitly marked for verification. The public policy page presents an operational disclosure while the legally reviewed version remains unpublished. The initial floating privacy prompt was removed; optional marketing and analytics remain independently controllable in the preferences center.

## Operational decisions still required

- Confirm legal entity/representative, RUT, domicile and privacy contact.
- Verify purposes and legal bases for every data set, including accounting, fraud prevention, analytics and promotions.
- Complete vendor agreements, processor/controller roles, processing countries, transfer safeguards and subprocessor lists.
- Approve retention periods and lawful exceptions for accounting, claims, active orders, backups and marketing evidence.
- Assign access roles and review MFA, session security, backup restore, incident escalation and deletion/backup lifecycle.
- Have Chilean privacy counsel validate the public policy and procedures before publication.
