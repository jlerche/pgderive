#!/usr/bin/env python3
"""Isolated release-only frame evaluator measurement, distinct from worker capacity."""
from frame_work_baseline import main


if __name__ == '__main__':
    main('target/extrema-work-baseline', 'measure_extrema_release_frames', 'EXTREMA_MEASUREMENT ',
         '1000 rows, NULL every seventh input, 19 integral values, filtered MIN/MAX/SUM/COUNT prefix frames; ROWS and GROUPS')
