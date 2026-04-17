# FilmuCore Rules

## Quality Bar Terminology

Interpret requests for "enterprise-grade" as an internal quality bar only.

Apply the standard through:

- robust architecture
- maintainable code
- strong typing
- accessibility
- security-minded defaults
- scalability
- testability
- clear separation of concerns

Do not use the word `enterprise` in code, docs, UI copy, filenames, component names, comments, documentation strings, CSS classes, or visible layout text unless the user explicitly asks for that exact word.

Prefer neutral wording such as:

- production-ready
- robust
- scalable
- high-quality
- professional
- advanced
- platform-grade

Before finishing any edit, audit added names and copy for this term and replace it with a neutral alternative when possible.

## Post-Push Follow-Through

After pushing changes to a remote review branch or PR branch:

- keep monitoring GitHub checks for red runs or newly failing jobs
- keep monitoring review conversations for unresolved threads or fresh feedback
- continue until checks are green and review conversations are resolved, or report the exact blocker if the branch is not yet merge-ready
