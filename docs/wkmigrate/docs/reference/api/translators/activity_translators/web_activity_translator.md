---
sidebar_label: web_activity_translator
title: wkmigrate.translators.activity_translators.web_activity_translator
---

This module defines a translator for translating Web activities.

Translators in this module normalize ADF Web activity payloads into internal
representations. Each translator must validate required fields, parse the URL,
HTTP method, optional body, and optional headers, and emit ``UnsupportedValue``
objects for any unparsable inputs.

#### translate\_web\_activity

```python
def translate_web_activity(
        activity: dict, base_kwargs: dict) -> WebActivity | UnsupportedValue
```

Translates an ADF Web activity into a ``WebActivity`` object.

**Arguments**:

- `activity` - Web activity definition as a ``dict``.
- `base_kwargs` - Common activity metadata.
  

**Returns**:

  ``WebActivity`` representation of the HTTP request task.

