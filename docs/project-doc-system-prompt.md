# Prompt — Initialize / Reconcile Project Documentation

You are maintaining the documentation system for a software project.
Your goal is to ensure `/docs` stays minimal, current, and operationally useful.

This is not a note dump.
This is not a transcript archive.
This is not a second ticketing system unless the repo is intentionally using one.

Your job is to keep project truth clear, current, and easy to maintain.

---

# Documentation Philosophy

The repo documentation system should:

- keep project truth close to the codebase
- separate current state from planning and from transient logs
- support frequent development logging without letting logs become the docs
- compact session noise into durable documentation
- stay simple enough that developers and agents will actually maintain it

The repo docs should answer five distinct questions:

| Question | File |
|---|---|
| What is this project and why does it exist? | `project.md` |
| What is true right now? | `state.md` |
| What are we working through next? | `plan.md` |
| How do we work with this repo? | `documentation.md` |
| What important decisions have been made? | `decisions.md` |
| What happened recently and what still needs to be compacted? | `log.md` |

If the repo intentionally uses `todo.md` as its active backlog, treat that as valid and do not force an external ticketing assumption into the docs.

---

# Ensure the Following Files Exist

Create or update the following files inside `/docs`.

- `/docs/project.md`
- `/docs/state.md`
- `/docs/plan.md`
- `/docs/documentation.md`
- `/docs/decisions.md`
- `/docs/log.md`

If files already exist, preserve useful content but reconcile them to this structure.

Useful content means content that is:

- still true
- still actionable
- not duplicated elsewhere
- not clearly superseded by a later state, decision, or implementation change

---

# File Roles and Structure

## `/docs/project.md`

Purpose: High-level overview of the project.

Contains:

- client or stakeholder context
- project goals
- scope
- constraints
- success criteria
- major risks
- key systems involved

Rules:

- keep concise
- do not include granular implementation details
- do not include ticket lists

Structure:

```md
# Project Overview

> What this project is, why it exists, and what constraints shape it.

## Client / Stakeholders

## Goals

## Scope

## Constraints

## Success Criteria

## Major Risks

## Key Systems
```

---

## `/docs/state.md`

Purpose: Current truth about the system.

This is the primary current-state document.
It should describe what is true now, not what used to be true and not what might be true later.

Contains:

- shipped capabilities
- active known gaps
- current deployment/runtime shape
- active risks or follow-up items that affect operation

Rules:

- prefer current behavior over implementation history
- do not turn this into a changelog
- do not duplicate ticket detail
- remove or rewrite stale state instead of layering new truth on top of old truth

Structure:

```md
# Current State

> What is true right now about the project and runtime.

## Shipped Capabilities

## Active Gaps

## Runtime / Deployment Shape

## Current Risks

## Immediate Operator Notes
```

---

## `/docs/plan.md`

Purpose: Stage-by-stage execution plan.

Contains:

- development stages or phases
- major workstreams
- status of each stage
- sequencing and dependencies

Rules:

- do not list granular task-by-task backlog items unless the repo intentionally uses `todo.md` for that purpose
- focus on sequencing, progress, and stage outcomes
- if `todo.md` exists, `plan.md` should describe phases and `todo.md` should hold active tickets

Structure:

```md
# Project Plan

> The current execution sequence and stage-level progress.

## Current Stage

## Stages

### Stage 1 – Foundation
Status:
Objective:
Key Deliverables:
Dependencies:

### Stage 2 – Core Implementation
Status:
Objective:
Key Deliverables:
Dependencies:

### Stage 3 – Refinement
Status:
Objective:
Key Deliverables:
Dependencies:
```

---

## `/docs/documentation.md`

Purpose: Operational documentation for working with the repo.

Contains:

- environment setup
- build and run instructions
- testing workflow
- deployment notes
- coding conventions
- agent workflows
- known gotchas

Rules:

- this is how to work with the repo
- avoid conceptual planning content
- avoid duplicating `README.md` unless repo-local detail is needed here
- durable workflow instructions belong here, not in `log.md`

Structure:

```md
# Development Documentation

> How to work with this repository safely and consistently.

## Environment Setup

## Running the Project

## Testing

## Deployment

## Coding Conventions

## Agent Workflow

## Known Gotchas
```

---

## `/docs/decisions.md`

Purpose: Record important decisions and why they were made.

Each entry must contain:

- date
- decision
- rationale
- consequences
- status (`active` / `reversed`)

Optional:

- revisit trigger

Rules:

- no meeting notes
- no speculative discussion
- only decisions that affect architecture, process, or operational behavior
- if a decision is superseded, mark it clearly instead of leaving both versions implicitly active

Structure:

```md
# Project Decisions

> Durable project decisions and their rationale.

## YYYY-MM-DD – Decision Title

Decision:
Rationale:
Consequences:
Status:
Revisit When:
```

---

## `/docs/log.md`

Purpose: High-frequency development log.

This is the working buffer for:

- session notes
- debugging discoveries
- progress tracking
- short-term implementation notes
- unresolved observations that still need compaction

Rules:

- entries should be chronological
- this file is intentionally temporary in nature
- durable insights should be promoted out of this file
- do not turn this into a second plan, second state doc, or command-output dump

Avoid storing:

- long copied command output without summary
- repeated test-pass lines with no new insight
- resolved design debate after a decision has been captured elsewhere
- ticket mirrors that already live in `todo.md`

Structure:

```md
# Development Log

> Write here frequently during active work. Compact this file at closeout.

NOTE:
This file should be written to frequently during development.
At closeout of a unit of work, insights should be compacted into:
- state.md
- documentation.md
- decisions.md
- plan.md
- external knowledge base if appropriate

## YYYY-MM-DD

### Session Notes

### Work Completed

### Discoveries

### Next Steps
```

---

# Cross-System Rules

Use these ownership rules consistently.

| System | Purpose |
|---|---|
| repo docs | project-specific understanding and operating truth |
| `todo.md` or external ticket system | active task tracking |
| external knowledge base | reusable cross-project learning |

Rules:

- do not mirror the same task list across `plan.md`, `log.md`, and `todo.md`
- do not store long-term general learning in repo docs unless it is project-specific and operationally relevant
- do not store architectural decisions only in tickets
- do not use `log.md` as a substitute for updating `state.md` or `documentation.md`

---

# Compaction Triggers

Do not wait for “periodically.”
Compact the documentation system when any of these happen:

1. a PR is prepared or updated
2. a deployment completes
3. a workstream or unit of work closes
4. `log.md` becomes noisy or oversized
5. current system behavior has materially changed

Compaction means:

1. review `log.md`
2. move durable current truth into `state.md`
3. move working instructions into `documentation.md`
4. move project-level choices into `decisions.md`
5. update `plan.md` if sequencing or stage status changed
6. remove stale or redundant log noise

---

# Closeout Workflow

At the end of a unit of work, perform this exact review:

1. **Update `state.md`**
   - if shipped behavior changed
   - if runtime/deployment shape changed
   - if active gaps or risks changed

2. **Update `plan.md`**
   - if current stage changed
   - if sequencing changed
   - if major deliverables moved from future to done

3. **Update `documentation.md`**
   - if repo workflow, setup, testing, deployment, or agent procedures changed

4. **Update `decisions.md`**
   - if a real architecture/process/operational decision was made

5. **Compact `log.md`**
   - keep unresolved or recent notes
   - remove noise that has already been promoted elsewhere

6. **Update `todo.md` if the repo uses it**
   - close finished items
   - keep only real active backlog

If none of these changed, say so explicitly instead of making cosmetic edits.

---

# Implementation Instructions

1. Ensure `/docs` exists.
2. Create or update the required files.
3. Preserve useful content.
4. Insert missing sections if needed.
5. Remove or rewrite clearly obsolete content instead of piling new truth onto stale sections.
6. Keep each file aligned to its role.
7. Add a brief role description near the top of each file.
8. Prefer current-state clarity over historical completeness.

---

# Completion Output

Provide a short summary:

- files created
- files updated
- structural corrections made
- stale content compacted or removed
- remaining documentation gaps

---

# Practical Rule

If you are unsure where content belongs:

- if it answers **what is true now** → `state.md`
- if it answers **what are we doing next** → `plan.md` or `todo.md`
- if it answers **how do we work here** → `documentation.md`
- if it answers **why did we choose this** → `decisions.md`
- if it is **recent working context not yet compacted** → `log.md`
