import json
import pandas as pd
import matplotlib.pyplot as plt

# Load JSON data
def load_json(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

# Extract run results
def parse_results(data):
    results = data.get('results', [])
    parsed_data = []
    for result in results:
        adapter_response = result.get('adapter_response', {})
        timing = result.get('timing', [])
        timing_data = {step['name']: step['completed_at'] for step in timing}

        parsed_data.append({
            'status': result.get('status'),
            'execution_time': result.get('execution_time'),
            'rows_affected': adapter_response.get('rows_affected', 0),
            'query_id': adapter_response.get('query_id', 'N/A'),
            'unique_id': result.get('unique_id'),
            'relation_name': result.get('relation_name'),
            **timing_data  # Include timing details as separate columns
        })
    return pd.DataFrame(parsed_data)

# Load and parse the file
file_path = '/mnt/data/run_results (7).json'
data = load_json(file_path)
parsed_results = parse_results(data)

# Display basic stats
def display_summary(df):
    print("Summary of Results:")
    print(df.describe(include='all'))
    print("\nExecution Time Stats:")
    print(df['execution_time'].describe())

# Plot rows affected
def plot_rows_affected(df):
    df['rows_affected'] = df['rows_affected'].astype(int)
    plt.figure(figsize=(10, 6))
    plt.barh(df['relation_name'], df['rows_affected'], color='skyblue')
    plt.xlabel('Rows Affected')
    plt.ylabel('Relation Name')
    plt.title('Rows Affected per Relation')
    plt.tight_layout()
    plt.show()

# Display
display_summary(parsed_results)
plot_rows_affected(parsed_results)

# Save to CSV for further analysis
output_csv = '/mnt/data/parsed_run_results.csv'
parsed_results.to_csv(output_csv, index=False)
print(f"Parsed results saved to {output_csv}")

# Display the longest running models
longest_running_models = parsed_results.sort_values(by='execution_time', ascending=False)[['relation_name', 'execution_time']]
print("\nLongest Running Models:")
print(longest_running_models.head(10))
