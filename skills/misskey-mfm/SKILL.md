---
name: misskey-mfm
description: "Misskey MFM syntax for formatting notes/chat (no tables)."
version: 1.0.0
author: Hermes Agent
license: MIT
metadata:
  hermes:
    tags: [misskey, mfm, formatting, social-media]
---

# Misskey MFM

Load this only when replying on Misskey and you want formatting beyond plain text.

## Not supported
- Tables, headings (`#`), and images via Markdown. Use plain lists with `-` or numbered lines instead.
- Keep it short; one or two effects at most. Plain text is usually best.

## Basics
- Bold: `**text**` / Strike: `~~text~~` / Small: `<small>text</small>` / Italic: `<i>text</i>`
- Center: `<center>text</center>`
- Inline code: `` `code` `` / Code block: three backticks (optionally with a language)
- Quote: `> text`
- Link: `[label](https://example.com)`; `?[label](url)` hides the link preview
- Mention: `@user` (local) or `@user@host` (remote) / Hashtag: `#tag`
- Custom emoji: `:name:` / Unicode emoji work as usual
- Math: `\( x^2 \)` inline, `\[ x^2 \]` block
- Escape MFM: wrap text in `<plain>...</plain>`

## Functions: `$[name text]`
- Effects: `tada`, `jelly`, `bounce`, `shake`, `spin`, `jump`, `rainbow`, `sparkle`, `flip`, `blur`
- Size: `$[x2 text]`, `$[x3 text]`, `$[x4 text]`
- Color: `$[fg.color=ff0000 text]`, `$[bg.color=ffff00 text]`
- Rotate: `$[rotate.deg=30 text]`
- Ruby: `$[ruby 漢字 かんじ]`
- Font: `$[font.serif text]`, `$[font.monospace text]`

Functions can be nested, e.g. `$[x2 $[tada Done!]]`. Other clients may not render them, so keep important text readable without them.
