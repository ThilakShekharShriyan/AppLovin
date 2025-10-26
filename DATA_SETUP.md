# Data Setup

The raw data (11GB) has been moved outside the project repository to `~/AppLovinData/raw` to keep the project lightweight.

## Current Setup

- **Raw data location**: `~/AppLovinData/raw/` (external to project)
- **Project symlink**: `data/raw` → `~/AppLovinData/raw/`
- **Generated data**: `data/lake/` and `data/mvs/` (project-local, gitignored)

## Why This Setup?

1. **Performance**: Heavy data operations don't slow down Git operations
2. **Portability**: Project can be cloned/moved without 11GB of data
3. **Sharing**: Raw data can be shared across multiple project instances
4. **Backup**: Raw data can be backed up independently

## Setup Instructions

If setting up on a new machine:

1. Create the data directory:
   ```bash
   mkdir -p ~/AppLovinData
   ```

2. Copy your raw CSV files to:
   ```bash
   ~/AppLovinData/raw/events_part_*.csv
   ```

3. Create the symlink (already done in current setup):
   ```bash
   ln -sf ~/AppLovinData/raw data/raw
   ```

4. Run the pipeline as usual:
   ```bash
   ./run_pipeline.sh
   ```

## Alternative Locations

To use a different location, update the symlink:
```bash
rm data/raw
ln -sf /path/to/your/data/raw data/raw
```