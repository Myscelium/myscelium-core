> SPDX-License-Identifier: MPL-2.0
> Copyright © 2021-2026 Cristian Camargo Filho

# History cleanup

Historical runtime SQLite buffers were previously committed and must be treated as exposed. Their contents are not republished.

The cleanup removes these data-bearing paths from all published branch history:

- `OxidizedMyscelium/Temp/buffer.db`
- `OxidizedMyscelium/Tempbuffer.db`

Repository ignore rules prevent database, SQLite journal, log, and temporary runtime files from being committed again.

Old clones remain contaminated. Contributors must reclone after rewritten refs are published and must not merge or push from old clones.
