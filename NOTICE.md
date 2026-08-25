# SirenaIX Messaging Gateway notices

SirenaIX Messaging Gateway is derived from the open-source
[`mautrix/gmessages`](https://github.com/mautrix/gmessages) project. The
SirenaIX work in this repository started from upstream release `v0.2608.0`,
commit `9743919f4884327db998fe0f227c073f3f3aceb3`. Original copyright and
license notices remain applicable to the upstream code.

Beginning in August 2026, SirenaIX contributors added a separately deployable,
multi-tenant messaging gateway, tenant-scoped contacts and aliases, durable
message delivery, media handling, webhooks, Kafka integration, operational
controls, and related tests and documentation. These modifications do not
change the origin or ownership of the upstream portions.

The combined work is distributed under the GNU Affero General Public License,
version 3 or later, as described in [`LICENSE`](LICENSE). SirenaIX has no special license exception.
[`LICENSE.exceptions`](LICENSE.exceptions) records
specific grants made by the upstream mautrix-gmessages developers to named
parties; merely using this fork does not make SirenaIX or another party a
beneficiary of those grants.

SirenaIX Messaging Gateway is an independent project. It is not affiliated with, endorsed by, or sponsored by Google.
Google Messages, Android, RCS, and
other product names may be trademarks of their respective owners.

Third-party modules and build tools retain their own copyright and license
terms. The committed `third_party/licenses` distribution bundle contains the
license texts, notices, and corresponding source emitted for the compiled Linux
gateway graph. Release archives and images include that reviewed bundle, and
container automation also produces a software bill of materials. Those
automated checks are useful evidence, not a guarantee of legal or regulatory
compliance.
