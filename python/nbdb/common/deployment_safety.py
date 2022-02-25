import pathlib
import subprocess
import sys


def runcmd(cmd: str):
    with subprocess.Popen(cmd, shell=True,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE) as process:
        out, err = process.communicate()
        return str(out, 'utf-8'), str(err, 'utf-8')


def assert_cwd_eq_prog_parent_path(prog_parent_abs_path: str):
    cwd = str(pathlib.Path().resolve())
    if cwd != prog_parent_abs_path:
        print(f'!!!\n'
              f'!!! Deployment Safety ERROR: working dir != prog parent path\n'
              f'!!!\n'
              f'!!!  prog parent path  : "{prog_parent_abs_path}"\n'
              f'!!!  working dir (cwd) : "{cwd}"\n'
              f'!!!',
              file=sys.stderr)
        assert False, "Deployment Safety ERROR (cwd)"


def assert_git_repo_state(last_verified_head_hash: str = ''):
    # 1. make sure we're on branch 'master'
    out, _ = runcmd('git rev-parse --abbrev-ref HEAD')
    current_branch = out.strip()
    if current_branch != 'master':
        print(f'!!!\n'
              f'!!! Deployment Safety ERROR: branch != master\n'
              f'!!!\n'
              f'!!!  current branch : {current_branch}\n'
              f'!!!',
              file=sys.stderr)
        assert False, "Deployment Safety ERROR (branch != master)"

    # 2. make sure there are no local changes pending
    out, _ = runcmd('git status --porcelain')
    if out:
        print(f'!!!\n'
              f'!!! Deployment Safety ERROR: local changes pending\n'
              f'!!!\n'
              f'!!!  see below output of git status --porcelain\n'
              f'!!!\n'
              f'{out}',
              file=sys.stderr)
        assert False, "Deployment Safety ERROR (local changes pending)"

    # 3. make sure that HEAD of master == HEAD of origin/master after fetch
    runcmd('git fetch')

    out, _ = runcmd('git rev-parse HEAD')
    master_head = out.strip()

    out, _ = runcmd('git show-ref origin/master | awk \'{print $1}\'')
    origin_master_head = out.strip()

    assert master_head and origin_master_head, "Failed to get git HEAD hash"

    if master_head != origin_master_head:
        print(f'!!!\n'
              f'!!! Deployment Safety ERROR: master != origin/master\n'
              f'!!!\n'
              f'!!!  master HEAD        : {master_head}\n'
              f'!!!  origin/master HEAD : {origin_master_head}\n'
              f'!!!',
              file=sys.stderr)
        assert False, "Deployment Safety ERROR (master != origin/master)"

    if last_verified_head_hash and master_head != last_verified_head_hash:
        print(f'!!!\n'
              f'!!! Deployment Safety ERROR: master HEAD not verified!\n'
              f'!!!\n'
              f'!!!  master HEAD          : {master_head}\n'
              f'!!!  last verified commit : {last_verified_head_hash}\n'
              f'!!!',
              file=sys.stderr)
        assert False, "Deployment Safety ERROR (master HEAD not verified)"
