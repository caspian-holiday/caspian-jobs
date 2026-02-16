"""
Script to generate example plots for statistical parameters used in TSFEL predictability classification.
This generates plots that will be referenced in the parameter documentation.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import tsfel
from pathlib import Path

# Create output directory
output_dir = Path(__file__).parent / 'parameter_examples'
output_dir.mkdir(exist_ok=True)

# Set style
plt.style.use('seaborn-v0_8-whitegrid')
figsize = (10, 6)

def generate_example_series():
    """Generate example time series with different characteristics."""
    np.random.seed(42)
    n = 100
    t = pd.date_range('2023-01-01', periods=n, freq='D')
    
    examples = {}
    
    # 1. High mean, low variance (stable)
    examples['high_mean_low_var'] = {
        'values': 100 + np.random.normal(0, 5, n),
        't': t,
        'label': 'High Mean, Low Variance'
    }
    
    # 2. Low mean, high variance (unstable)
    examples['low_mean_high_var'] = {
        'values': 10 + np.random.normal(0, 20, n),
        't': t,
        'label': 'Low Mean, High Variance'
    }
    
    # 3. High ACF1 (strong autocorrelation)
    examples['high_acf1'] = {
        'values': np.cumsum(np.random.normal(0, 1, n)) + 50,
        't': t,
        'label': 'High ACF1 (Strong Autocorrelation)'
    }
    
    # 4. Low ACF1 (weak autocorrelation, noise)
    examples['low_acf1'] = {
        'values': np.random.normal(50, 10, n),
        't': t,
        'label': 'Low ACF1 (Weak Autocorrelation)'
    }
    
    # 5. Positive skewness
    examples['positive_skew'] = {
        'values': np.random.gamma(2, 2, n) * 10,
        't': t,
        'label': 'Positive Skewness'
    }
    
    # 6. Negative skewness
    examples['negative_skew'] = {
        'values': 100 - np.random.gamma(2, 2, n) * 10,
        't': t,
        'label': 'Negative Skewness'
    }
    
    # 7. High kurtosis (heavy tails)
    examples['high_kurtosis'] = {
        'values': np.concatenate([
            np.random.normal(50, 5, n-10),
            np.random.normal(50, 30, 10)  # Outliers
        ]),
        't': t,
        'label': 'High Kurtosis (Heavy Tails)'
    }
    
    # 8. High zero crossing rate (oscillatory)
    examples['high_zcr'] = {
        'values': 50 + 20 * np.sin(np.linspace(0, 10*np.pi, n)) + np.random.normal(0, 2, n),
        't': t,
        'label': 'High Zero Crossing Rate'
    }
    
    # 9. Strong trend (high slope)
    examples['strong_trend'] = {
        'values': np.linspace(10, 100, n) + np.random.normal(0, 3, n),
        't': t,
        'label': 'Strong Trend (High Slope)'
    }
    
    # 10. Seasonal pattern (weekly)
    examples['seasonal'] = {
        'values': 50 + 10 * np.sin(2 * np.pi * np.arange(n) / 7) + np.random.normal(0, 2, n),
        't': t,
        'label': 'Weekly Seasonality'
    }
    
    # 11. High stability (low MAD)
    examples['high_stability'] = {
        'values': 50 + np.random.normal(0, 1, n),
        't': t,
        'label': 'High Stability (Low MAD)'
    }
    
    # 12. Low stability (high MAD)
    examples['low_stability'] = {
        'values': 50 + np.random.laplace(0, 10, n),
        't': t,
        'label': 'Low Stability (High MAD)'
    }
    
    return examples

def calculate_features(values):
    """Calculate all features for a time series."""
    signal = np.array(values)
    signal = signal[~np.isnan(signal)]
    
    if len(signal) < 10:
        return {}
    
    try:
        cfg = tsfel.get_features_by_domain(['statistical', 'temporal'])
        feature_dict = tsfel.time_series_features_extractor(cfg, signal, fs=1.0)
        
        # Calculate ACF1 manually
        if len(signal) > 1:
            acf1 = np.corrcoef(signal[:-1], signal[1:])[0, 1]
        else:
            acf1 = np.nan
        
        features = {
            'mean': np.mean(signal),
            'std': np.std(signal),
            'var': np.var(signal),
            'cv': np.std(signal) / np.mean(signal) if np.mean(signal) != 0 else np.inf,
            'acf1': acf1,
            'autocorr_persistence': feature_dict.get('0_Autocorrelation', np.nan),
            'skewness': feature_dict.get('0_Skewness', np.nan),
            'kurtosis': feature_dict.get('0_Kurtosis', np.nan),
            'slope': feature_dict.get('0_Slope', np.nan),
            'trend_strength': abs(feature_dict.get('0_Slope', 0)),
            'mean_absolute_deviation': feature_dict.get('0_Mean absolute deviation', np.nan),
            'zero_crossing_rate': feature_dict.get('0_Zero crossing rate', np.nan),
            'interquartile_range': feature_dict.get('0_Interquartile range', np.nan),
            'entropy': feature_dict.get('0_Entropy', np.nan),
        }
        
        # Calculate stability
        mad = features.get('mean_absolute_deviation', np.nan)
        mean_val = features.get('mean', np.nan)
        if not np.isnan(mad) and not np.isnan(mean_val) and abs(mean_val) > 0:
            normalized_mad = mad / abs(mean_val)
            features['stability'] = 1.0 / (1.0 + normalized_mad) if normalized_mad > 0 else 1.0
        else:
            features['stability'] = np.nan
        
        return features
    except:
        return {}

def plot_parameter_examples():
    """Generate example plots for each parameter."""
    examples = generate_example_series()
    
    # Plot each example series
    for key, data in examples.items():
        fig, ax = plt.subplots(figsize=figsize)
        ax.plot(data['t'], data['values'], 'b-', linewidth=2, alpha=0.7)
        ax.axhline(y=np.mean(data['values']), color='r', linestyle='--', label=f"Mean: {np.mean(data['values']):.2f}")
        ax.set_title(data['label'], fontsize=14, fontweight='bold')
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Value', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(output_dir / f'{key}.png', dpi=150, bbox_inches='tight')
        plt.close()
    
    # Create comparison plots for key parameters
    create_comparison_plots(examples)
    
    print(f"Generated {len(examples)} example plots in {output_dir}")

def create_comparison_plots(examples):
    """Create side-by-side comparison plots for key parameters."""
    
    # ACF1 comparison
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    for idx, key in enumerate(['high_acf1', 'low_acf1']):
        data = examples[key]
        features = calculate_features(data['values'])
        ax = axes[idx]
        ax.plot(data['t'], data['values'], 'b-', linewidth=2, alpha=0.7)
        ax.set_title(f"{data['label']}\nACF1 = {features.get('acf1', 0):.3f}", 
                    fontsize=12, fontweight='bold')
        ax.set_xlabel('Date', fontsize=10)
        ax.set_ylabel('Value', fontsize=10)
        ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_dir / 'acf1_comparison.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # Skewness comparison
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    for idx, key in enumerate(['positive_skew', 'negative_skew']):
        data = examples[key]
        features = calculate_features(data['values'])
        ax = axes[idx]
        ax.plot(data['t'], data['values'], 'b-', linewidth=2, alpha=0.7)
        ax.set_title(f"{data['label']}\nSkewness = {features.get('skewness', 0):.3f}", 
                    fontsize=12, fontweight='bold')
        ax.set_xlabel('Date', fontsize=10)
        ax.set_ylabel('Value', fontsize=10)
        ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_dir / 'skewness_comparison.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # Stability comparison
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    for idx, key in enumerate(['high_stability', 'low_stability']):
        data = examples[key]
        features = calculate_features(data['values'])
        ax = axes[idx]
        ax.plot(data['t'], data['values'], 'b-', linewidth=2, alpha=0.7)
        ax.axhline(y=features.get('mean', 0), color='r', linestyle='--', 
                  label=f"Mean: {features.get('mean', 0):.2f}")
        ax.set_title(f"{data['label']}\nStability = {features.get('stability', 0):.3f}", 
                    fontsize=12, fontweight='bold')
        ax.set_xlabel('Date', fontsize=10)
        ax.set_ylabel('Value', fontsize=10)
        ax.legend()
        ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_dir / 'stability_comparison.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # CV comparison
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    for idx, key in enumerate(['high_mean_low_var', 'low_mean_high_var']):
        data = examples[key]
        features = calculate_features(data['values'])
        ax = axes[idx]
        ax.plot(data['t'], data['values'], 'b-', linewidth=2, alpha=0.7)
        ax.axhline(y=features.get('mean', 0), color='r', linestyle='--', 
                  label=f"Mean: {features.get('mean', 0):.2f}, CV: {features.get('cv', 0):.3f}")
        ax.set_title(f"{data['label']}\nCV = {features.get('cv', 0):.3f}", 
                    fontsize=12, fontweight='bold')
        ax.set_xlabel('Date', fontsize=10)
        ax.set_ylabel('Value', fontsize=10)
        ax.legend()
        ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_dir / 'cv_comparison.png', dpi=150, bbox_inches='tight')
    plt.close()

if __name__ == '__main__':
    plot_parameter_examples()
    print("Example plots generated successfully!")

