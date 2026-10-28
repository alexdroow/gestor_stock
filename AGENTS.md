# Project Workflow

- After implementing and verifying a requested change, commit it and push it to the current GitHub branch so it is available for deployment.
- Stage only files related to the current task. Never include unrelated or pre-existing working-tree changes in the commit.
- If tests fail, the push is rejected, or the working tree contains unexpected changes, stop and report the issue instead of claiming the changes are uploaded.
- Report the pushed commit hash and provide the PythonAnywhere pull/reload steps when the change needs deployment there.
