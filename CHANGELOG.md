# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.1.0](https://github.com/mlcast-community/mlcast-dataset-radklim/releases/tag/v0.1.0)

First tagged release of zarr version of the RadKlim dataset including both hour
and 5-minute data, and up to Extension 6 (so that the total timespan covered is
2001-2023). Includes fixes to projection `crs_wkt` string to set x/y coord
units correctly (as `km` rather than `m`) and domain bounds (which are required
when `cartopy` when using the projection for plotting).
