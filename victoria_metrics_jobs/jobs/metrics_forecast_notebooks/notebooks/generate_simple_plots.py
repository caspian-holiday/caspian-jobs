"""
Simple script to generate example plots for statistical parameters.
This version doesn't require TSFEL - just demonstrates concepts visually.
"""

import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# Create output directory
output_dir = Path(__file__).parent / 'parameter_examples'
output_dir.mkdir(exist_ok=True)

np.random.seed(42)
n = 100
t = np.arange(n)

# Generate example series
examples = {
    'high_mean_low_var': {
        'values': 100 + np.random.normal(0, 5, n),
        'label': 'High Mean, Low Variance\nMean=100, Std=5, CV=0.05'
    },
    'low_mean_high_var': {
        'values': 10 + np.random.normal(0, 20, n),
        'label': 'Low Mean, High Variance\nMean=10, Std=20, CV=2.0'
    },
    'high_acf1': {
        'values': np.cumsum(np.random.normal(0, 1, n)) + 50,
        'label': 'High ACF1 (Strong Autocorrelation)\nACF1 ≈ 0.95'
    },
    'low_acf1': {
        'values': np.random.normal(50, 10, n),
        'label': 'Low ACF1 (Weak Autocorrelation)\nACF1 ≈ 0.05'
    },
    'positive_skew': {
        'values': np.random.gamma(2, 2, n) * 10,
        'label': 'Positive Skewness\nSkewness ≈ 1.4'
    },
    'negative_skew': {
        'values': 100 - np.random.gamma(2, 2, n) * 10,
        'label': 'Negative Skewness\nSkewness ≈ -1.4'
    },
    'high_kurtosis': {
        'values': np.concatenate([
            np.random.normal(50, 5, n-10),
            np.random.normal(50, 30, 10)
        ]),
        'label': 'High Kurtosis (Heavy Tails)\nKurtosis ≈ 6.5'
    },
    'high_zcr': {
        'values': 50 + 20 * np.sin(np.linspace(0, 10*np.pi, n)) + np.random.normal(0, 2, n),
        'label': 'High Zero Crossing Rate\nOscillatory Pattern'
    },
    'strong_trend': {
        'values': np.linspace(10, 100, n) + np.random.normal(0, 3, n),
        'label': 'Strong Trend (High Slope)\nSlope ≈ 0.9'
    },
    'seasonal': {
        'values': 50 + 10 * np.sin(2 * np.pi * np.arange(n) / 7) + np.random.normal(0, 2, n),
        'label': 'Weekly Seasonality\nPeriod = 7 days'
    },
    'high_stability': {
        'values': 50 + np.random.normal(0, 1, n),
        'label': 'High Stability (Low MAD)\nStability ≈ 0.95'
    },
    'low_stability': {
        'values': 50 + np.random.laplace(0, 10, n),
        'label': 'Low Stability (High MAD)\nStability ≈ 0.4'
    }
}

# Generate individual plots
for key, data in examples.items():
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(t, data['values'], 'b-', linewidth=2, alpha=0.7)
    mean_val = np.mean(data['values'])
    ax.axhline(y=mean_val, color='r', linestyle='--', linewidth=2, 
               label=f'Mean: {mean_val:.2f}')
    ax.set_title(data['label'], fontsize=14, fontweight='bold')
    ax.set_xlabel('Time Index', fontsize=12)
    ax.set_ylabel('Value', fontsize=12)
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_dir / f'{key}.png', dpi=150, bbox_inches='tight')
    plt.close()

# Create comparison plots
fig, axes = plt.subplots(1, 2, figsize=(16, 6))
for idx, key in enumerate(['high_acf1', 'low_acf1']):
    data = examples[key]
    ax = axes[idx]
    ax.plot(t, data['values'], 'b-', linewidth=2, alpha=0.7)
    ax.set_title(data['label'], fontsize=12, fontweight='bold')
    ax.set_xlabel('Time Index', fontsize=10)
    ax.set_ylabel('Value', fontsize=10)
    ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(output_dir / 'acf1_comparison.png', dpi=150, bbox_inches='tight')
plt.close()

fig, axes = plt.subplots(1, 2, figsize=(16, 6))
for idx, key in enumerate(['positive_skew', 'negative_skew']):
    data = examples[key]
    ax = axes[idx]
    ax.plot(t, data['values'], 'b-', linewidth=2, alpha=0.7)
    mean_val = np.mean(data['values'])
    ax.axhline(y=mean_val, color='r', linestyle='--', linewidth=2)
    ax.set_title(data['label'], fontsize=12, fontweight='bold')
    ax.set_xlabel('Time Index', fontsize=10)
    ax.set_ylabel('Value', fontsize=10)
    ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(output_dir / 'skewness_comparison.png', dpi=150, bbox_inches='tight')
plt.close()

fig, axes = plt.subplots(1, 2, figsize=(16, 6))
for idx, key in enumerate(['high_stability', 'low_stability']):
    data = examples[key]
    ax = axes[idx]
    ax.plot(t, data['values'], 'b-', linewidth=2, alpha=0.7)
    mean_val = np.mean(data['values'])
    ax.axhline(y=mean_val, color='r', linestyle='--', linewidth=2, 
              label=f'Mean: {mean_val:.2f}')
    ax.set_title(data['label'], fontsize=12, fontweight='bold')
    ax.set_xlabel('Time Index', fontsize=10)
    ax.set_ylabel('Value', fontsize=10)
    ax.legend()
    ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(output_dir / 'stability_comparison.png', dpi=150, bbox_inches='tight')
plt.close()

fig, axes = plt.subplots(1, 2, figsize=(16, 6))
for idx, key in enumerate(['high_mean_low_var', 'low_mean_high_var']):
    data = examples[key]
    ax = axes[idx]
    ax.plot(t, data['values'], 'b-', linewidth=2, alpha=0.7)
    mean_val = np.mean(data['values'])
    std_val = np.std(data['values'])
    cv = std_val / mean_val if mean_val != 0 else np.inf
    ax.axhline(y=mean_val, color='r', linestyle='--', linewidth=2, 
              label=f'Mean: {mean_val:.2f}, CV: {cv:.3f}')
    ax.set_title(data['label'], fontsize=12, fontweight='bold')
    ax.set_xlabel('Time Index', fontsize=10)
    ax.set_ylabel('Value', fontsize=10)
    ax.legend()
    ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(output_dir / 'cv_comparison.png', dpi=150, bbox_inches='tight')
plt.close()

print(f"Generated {len(examples)} example plots and 4 comparison plots in {output_dir}")

