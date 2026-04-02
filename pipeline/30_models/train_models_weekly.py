# train_models_weekly.py
#
# Weekly model training script:
# - Pulls model_games_enriched dataset
# - Trains multiple models (Ridge, SVR, RF, XGB)
# - Runs light grid search for each
# - Saves best models + metadata to disk
#
# Basically retrains everything from scratch on latest data and updates "latest" models
#
# Usage:
#   python train_models_weekly.py
#
# Notes:
# - Uses simple train/val split (not time-based for now)
# - Grid searches are intentionally tighter so this can run weekly without taking forever
# - Outputs overwrite previous models in /latest

import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import joblib
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))  # allow config import when run directly
from config import CFG

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import Ridge
from sklearn.svm import SVR
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score

try:
    from xgboost import XGBRegressor
except Exception as e:
    raise RuntimeError("xgboost not installed. pip install xgboost") from e


def iso_now_utc() -> str:
    # consistent timestamp for model metadata
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_dir(p: Path) -> None:
    # create directory if it doesn't exist
    p.mkdir(parents=True, exist_ok=True)


def main():
    run_utc = iso_now_utc()

    # ----------------------------
    # Load training data (CURRENT dataset)
    # ----------------------------
    conn = sqlite3.connect(CFG.DB_PATH)
    mg = pd.read_sql_query("SELECT * FROM model_games_enriched;", conn)
    conn.close()

    features = list(CFG.FEATURES)

    # drop any rows missing features or target
    df = mg.dropna(subset=features + ["home_margin"]).copy()

    if len(df) < int(getattr(CFG, "MIN_TRAIN_ROWS", 500)):
        raise RuntimeError(f"Not enough rows in model_games_enriched to train (have {len(df)}).")

    X = df[features]
    y = df["home_margin"].astype(float).values  # regression target

    # simple random split (good enough for now, may switch to time-based later)
    X_train, X_val, y_train, y_val = train_test_split(
        X,
        y,
        test_size=float(getattr(CFG, "TRAIN_VAL_SPLIT", 0.20)),
        random_state=int(getattr(CFG, "TRAIN_RANDOM_STATE", 42)),
    )

    n_jobs_cv = int(getattr(CFG, "N_JOBS_CV", 4))

    # ----------------------------
    # Ridge (scaled) + GridSearch
    # ----------------------------
    print("TRAINING RIDGE...")
    ridge_pipe = Pipeline([("scaler", StandardScaler()), ("model", Ridge())])
    ridge_grid = {"model__alpha": np.logspace(-6, 6, 13)}

    ridge_cv = GridSearchCV(
        ridge_pipe,
        ridge_grid,
        cv=5,
        scoring="neg_mean_absolute_error",
        n_jobs=n_jobs_cv,
        verbose=0
    )
    ridge_cv.fit(X_train, y_train)

    ridge_best = ridge_cv.best_estimator_
    ridge_pred = ridge_best.predict(X_val)
    ridge_mae = mean_absolute_error(y_val, ridge_pred)
    ridge_r2 = r2_score(y_val, ridge_pred)

    # ----------------------------
    # SVR (scaled) + GridSearch
    # (kept tighter than older versions so weekly training is realistic)
    # ----------------------------
    print("TRAINING SVR...")
    svr_pipe = Pipeline([("scaler", StandardScaler()), ("model", SVR())])
    svr_grid = {
        "model__kernel": ["rbf", "linear"],
        "model__C": [0.5, 1, 3, 10],
        "model__epsilon": [0.05, 0.1, 0.2],
        "model__gamma": ["scale", "auto"],
    }

    svr_cv = GridSearchCV(
        svr_pipe,
        svr_grid,
        cv=5,
        scoring="neg_mean_absolute_error",
        n_jobs=n_jobs_cv,
        verbose=0
    )
    svr_cv.fit(X_train, y_train)

    svr_best = svr_cv.best_estimator_
    svr_pred = svr_best.predict(X_val)
    svr_mae = mean_absolute_error(y_val, svr_pred)
    svr_r2 = r2_score(y_val, svr_pred)

    # ----------------------------
    # Random Forest (unscaled) + GridSearch
    # ----------------------------
    print("TRAINING RANDOM FOREST...")
    rf = RandomForestRegressor(
        random_state=int(getattr(CFG, "RF_RANDOM_STATE", 42)),
        n_jobs=int(getattr(CFG, "RF_NJOBS", 4)),
    )

    rf_grid = {
        "n_estimators": [300, 600],
        "max_depth": [None, 10, 20],
        "min_samples_split": [2, 5],
        "min_samples_leaf": [1, 2],
        "max_features": ["sqrt", "log2"],
        "bootstrap": [True],
    }

    rf_cv = GridSearchCV(
        rf,
        rf_grid,
        cv=5,
        scoring="neg_mean_absolute_error",
        n_jobs=n_jobs_cv,
        verbose=0
    )
    rf_cv.fit(X_train, y_train)

    rf_best = rf_cv.best_estimator_
    rf_pred = rf_best.predict(X_val)
    rf_mae = mean_absolute_error(y_val, rf_pred)
    rf_r2 = r2_score(y_val, rf_pred)

    # ----------------------------
    # XGB (unscaled) + GridSearch
    # ----------------------------
    print("TRAINING XGB...")

    # convert to numpy for xgboost (more stable + faster)
    X_train_np = X_train.to_numpy(dtype=np.float32)
    X_val_np   = X_val.to_numpy(dtype=np.float32)
    y_train_np = y_train.astype(np.float32)
    y_val_np   = y_val.astype(np.float32)

    xgb = XGBRegressor(
        objective="reg:squarederror",
        random_state=int(getattr(CFG, "XGB_RANDOM_STATE", 42)),
        n_jobs=int(getattr(CFG, "XGB_N_JOBS", 4)),
    )

    xgb_grid = {
        "n_estimators": [400, 800],
        "max_depth": [3, 4, 5],
        "learning_rate": [0.02, 0.05, 0.1],
        "subsample": [0.85, 1.0],
        "colsample_bytree": [0.85, 1.0],
        "reg_alpha": [0.0, 0.1],
        "reg_lambda": [1.0, 2.0],
    }

    xgb_cv = GridSearchCV(
        xgb,
        xgb_grid,
        cv=5,
        scoring="neg_mean_absolute_error",
        n_jobs=n_jobs_cv,
        verbose=0
    )
    xgb_cv.fit(X_train_np, y_train_np)

    xgb_best = xgb_cv.best_estimator_

    xgb_pred = xgb_best.predict(X_val_np)
    xgb_mae = mean_absolute_error(y_val_np, xgb_pred)
    xgb_r2 = r2_score(y_val_np, xgb_pred)

    # ----------------------------
    # Save artifacts (latest)
    # ----------------------------
    print("SAVING MODELS...")
    out_dir = Path(r"C:\GoopNet\pipeline\30_models\latest")  # central "latest" model folder
    ensure_dir(out_dir)

    joblib.dump(ridge_best, out_dir / "ridge.joblib")
    joblib.dump(svr_best, out_dir / "svr.joblib")
    joblib.dump(rf_best, out_dir / "rf.joblib")
    joblib.dump(xgb_best, out_dir / "xgb.joblib")

    (out_dir / "feature_list.json").write_text(json.dumps(features, indent=2))

    # store metrics + params so I can track performance over time
    meta = {
        "trained_at_utc": run_utc,
        "n_rows": int(len(df)),
        "n_train": int(len(X_train)),
        "n_val": int(len(X_val)),
        "metrics": {
            "ridge": {"mae": float(ridge_mae), "r2": float(ridge_r2), "best_params": ridge_cv.best_params_},
            "svm": {"mae": float(svr_mae), "r2": float(svr_r2), "best_params": svr_cv.best_params_},
            "random_forest": {"mae": float(rf_mae), "r2": float(rf_r2), "best_params": rf_cv.best_params_},
            "xgb": {"mae": float(xgb_mae), "r2": float(xgb_r2), "best_params": xgb_cv.best_params_},
        },
    }

    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    print("Saved models to:", out_dir)
    print(json.dumps(meta["metrics"], indent=2))


if __name__ == "__main__":
    main()