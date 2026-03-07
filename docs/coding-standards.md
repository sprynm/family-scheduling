# Coding Standards

JavaScript rules for this codebase. Apply to all files under `src/`.

---

## JS — Required Rules

### No `var` — use `const` or `let`
`var` has function scope and hoists in ways that cause hard-to-find bugs. Always use `const` (preferred) or `let`.

```js
// ❌
var result = await repo.listSources();

// ✅
const result = await repo.listSources();
```

---

### Always use `===`, never `==`
`==` performs type coercion. `0 == false` is `true`. `null == undefined` is `true`. These are traps.

```js
// ❌
if (status == 'ok')

// ✅
if (status === 'ok')
```

---

### Always `await` async calls — never fire-and-forget without a catch
Missing `await` means working with a Promise object instead of the resolved value.

```js
// ❌ — result is a Promise, not the data
const data = repo.listSources();

// ✅
const data = await repo.listSources();
```

---

### All async code must handle errors

Every `async` function called from an event handler or top-level context must have either a `try/catch` or a `.catch()`. Unhandled promise rejections crash silently in the browser and fatally in Node.

```js
// ❌ — rejection swallowed silently
btn.addEventListener('click', async () => {
  const data = await fetchJson('/api/sources');
  render(data);
});

// ✅
btn.addEventListener('click', async () => {
  try {
    const data = await fetchJson('/api/sources');
    render(data);
  } catch (err) {
    setStatus('Failed: ' + err.message, true);
  }
});
```

For Promise chains, always add `.catch()`:

```js
// ❌
navigator.clipboard.writeText(url).then(() => { btn.textContent = 'Copied!'; });

// ✅
navigator.clipboard.writeText(url)
  .then(() => { btn.textContent = 'Copied!'; })
  .catch(() => { btn.textContent = 'Failed'; });
```

---

### Always return from `map()`, `filter()`, `reduce()` callbacks
Arrow functions without braces implicitly return. Arrow functions with braces require an explicit `return`. Omitting it returns `undefined` for every element.

```js
// ❌ — returns undefined for every row
const rows = events.map((e) => {
  '<tr><td>' + e.title + '</td></tr>';
});

// ✅ — implicit return (no braces)
const rows = events.map((e) => '<tr><td>' + e.title + '</td></tr>');

// ✅ — explicit return (with braces)
const rows = events.map((e) => {
  return '<tr><td>' + e.title + '</td></tr>';
});
```

---

### No magic numbers — use named constants or env vars
Inline numeric literals are unexplained and unmaintainable. Put them in `constants.js` or reference env vars.

```js
// ❌
setTimeout(() => { btn.textContent = orig; }, 1500);
'cache-control': 'public, max-age=300'

// ✅ — for durations visible to users, a comment is the minimum:
setTimeout(() => { btn.textContent = orig; }, 1500); // 1.5s reset
// or extract:
const COPY_RESET_MS = 1500;
```

For server-side values like cache duration, prefer env vars (`env.FEED_CACHE_MAX_AGE`) or a named constant in `constants.js`.

---

## JS — Patterns to Avoid

### No `function()` callbacks where `this` matters
Classic `function()` syntax rebinds `this`. Inside event handlers, `setTimeout`, or array methods, `this` will not be the expected object. Use arrow functions.

```js
// ❌ — this is window or undefined
setTimeout(function() { this.render(); }, 100);

// ✅
setTimeout(() => { this.render(); }, 100);
```

This is not currently a problem in this codebase (arrow functions are used throughout) but must be maintained as new code is added.

---

### No floating-point arithmetic for money or counts
JavaScript's IEEE 754 floating point is imprecise: `0.1 + 0.2 === 0.30000000000000004`. This codebase does not do financial arithmetic, but any future numeric aggregation (durations, counts) should use integer arithmetic or explicit rounding.

---

## SQL / D1 Rules

### No `BEGIN` / `COMMIT` / `ROLLBACK`
D1 does not support SQL transactions. They throw `D1_EXEC_ERROR` at runtime. Use `db.batch([stmt1, stmt2])` for multi-statement atomicity.

```js
// ❌ — crashes on D1
await this.db.exec('BEGIN');
await this.db.prepare('DELETE ...').run();
await this.db.prepare('INSERT ...').run();
await this.db.exec('COMMIT');

// ✅
await this.db.batch([
  this.db.prepare('DELETE ...').bind(...),
  this.db.prepare('INSERT ...').bind(...),
]);
```

**Ref:** `TECH_DEBT.md`, `C:\Dev\Agents\antipatterns\d1-sql-transactions.md`

---

### Bind all values — no string interpolation in SQL
Never build SQL by concatenating user input. Always use `?` placeholders and `.bind()`.

```js
// ❌ — SQL injection risk
await this.db.prepare(`SELECT * FROM sources WHERE id = '${id}'`).run();

// ✅
await this.db.prepare('SELECT * FROM sources WHERE id = ?').bind(id).run();
```

---

## General

- Prefer `const` everywhere; use `let` only when the variable is reassigned
- No unused variables — remove them rather than prefixing with `_`
- No commented-out code — delete it; git history preserves it
- No `console.log` left in production paths — use structured error responses instead
