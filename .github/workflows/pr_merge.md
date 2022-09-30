name: PR_Merge 

on:
  push:
    branches: [master]
  workflow_dispatch:

jobs:
  generate_changelog:
    name: Generate changelog
    runs-on: ubuntu-latest
    steps:
    - name: Checkout
      uses: actions/checkout@v3
      with:
        fetch-depth: 0
    - name: Generate a changelog
      uses: orhun/git-cliff-action@v1
      id: git-cliff
      with:
        config: cliff.toml
        args: --verbose
      env:
        OUTPUT: CHANGELOG.md
    - uses: actions/upload-artifact@v3
      with:
        name: CHANGELOG.md
        path: CHANGELOG.md 
  squash_to_previous_commit:
    name: Add generated files to PR commit
    runs-on: ubuntu-latest
    needs:
      - generate_changelog
    steps:
    - name: Checkout
      uses: actions/checkout@v3
      with:
        fetch-depth: 0
        token: ${{ secrets.CI_COMMIT_PUSH_PAT }}
    - name: Git branch name
      id: git-branch-name
      uses: EthanSK/git-branch-name-action@v1
    - uses: actions/download-artifact@v3
      with:
        name: CHANGELOG.md
        path: .
    - name: Configure git user 
      run: |
        git status
        git config user.name "github-actions[bot]"
        git config user.email "github-actions[bot]@users.noreply.github.com"
    - name: Add updates and rewrite commit message to branch ${{ env.GIT_BRANCH_NAME }}
      run: |
        git reset --soft HEAD~1
        
        # Note: github.event.head_commit.message won't have a value if job started manually
        git commit --all --message "${{ github.event.head_commit.message }}
        
        Update CHANGELOG.md from Github Actions
        [actions skip]"
        git status
        git log -5
    - name: Pushing to the protected branch ${{ env.GIT_BRANCH_NAME }}
      uses: ad-m/github-push-action@master
      with:
        github_token: ${{ secrets.CI_COMMIT_PUSH_PAT }}
        branch: ${{ env.GIT_BRANCH_NAME }}
        force_with_lease: true