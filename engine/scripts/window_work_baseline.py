#!/usr/bin/env python3
"""Isolated release-only frame evaluator measurement, distinct from worker capacity."""
from frame_work_baseline import main


if __name__ == '__main__':
    main('target/window-work-baseline', 'measure_sliding_release_frames', 'WINDOW_MEASUREMENT ',
         '1000 rows, NULL every seventh input, 19 integral values, COUNT/SUM/AVG prefix frames; ROWS and GROUPS')
