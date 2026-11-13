#!/usr/bin/env python3
"""Focused live boundaries; semantic matrices run in virtual-clock Rust replay."""
import pathlib
import sys


def run(name, output, command):
    if name == 'memtable':
        from memtable_harness import qualify
        qualify(output, command)
    elif name == 'checkpoint':
        from async_checkpoint_harness import case
        case(output/'during-upload', command, False)
    elif name == 'lifecycle':
        from query_lifecycle_harness import qualify, grouped
        directory=output/'grouped'; directory.mkdir()
        qualify(directory, command, grouped)
    else:
        raise ValueError('Unknown core boundary: '+name)


if __name__ == '__main__':
    name, directory, *command = sys.argv[1:]
    if command[:1] == ['--']:
        command = command[1:]
    assert command
    run(name,pathlib.Path(directory),command)
