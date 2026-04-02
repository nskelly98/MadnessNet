# MadnessNet: College Basketball Prediction Pipeline

End-to-end machine learning pipeline for predicting college basketball game margins and generating betting recommendations.

## Overview

This project builds a full workflow from raw game data → feature engineering → model training → live predictions → bet tracking and reporting.

The system is designed to:
- Continuously ingest game data
- Train models on rolling team performance
- Generate daily betting recommendations
- Track performance and ROI over time

## Pipeline

### 1. Data Ingestion
- Pulls NCAA game results and team stats
- Stores structured data in SQLite

### 2. Feature Engineering
- Rolling statistics (5-game, 10-game, EWM)
- Trend metrics
- Home vs away deltas

### 3. Model Training
- Models:
  - Ridge Regression
  - Support Vector Regression (SVR)
  - Random Forest
  - XGBoost
- Ensemble of models used for final prediction

### 4. Prediction & Betting Logic
- Combines model outputs into ensemble prediction
- Calculates betting edge vs market spread
- Applies thresholds and risk controls:
  - Max bets per day
  - Max units per bet
  - Exposure caps

### 5. Evaluation
- Grades bets against final game results
- Tracks:
  - Win/Loss/Push
  - Units won/lost
  - ROI

### 6. Reporting
- Generates daily reports
- Sends results to Discord webhook

## Example Output

- Predicted margin: +6.5
- Market spread: -3.5
- Edge: +3.0 → qualifies as bet

## Tech Stack

- Python
- Pandas / NumPy
- Scikit-learn
- XGBoost
- SQLite
- RapidFuzz (team name matching)

## Notes

- No betting lines are used in training (avoids leakage)
- Designed for incremental updates and daily automation
- Includes fallback handling for missing data and mapping issues

## Future Improvements

- Time-based cross-validation
- Closing Line Value (CLV) tracking
- Market-aware calibration
- Deployment / API layer
