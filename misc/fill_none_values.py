import pandas as pd
import os

def fill_none_grant_ids():
    """
    Read the grant_nodes.csv file, replace 'None' values in grant_id column with
    a concatenation of country and agency, and save to a new file.
    """
    # Define file paths
    input_file = '/Users/gopinath.balu/Workspace/agentic_ai_innovations/constructed_KG/csv/nodes/grant_nodes.csv'
    output_file = input_file.replace('.csv', '_new.csv')
    
    print(f"Starting to process: {input_file}")
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    print(f"Read {len(df)} rows from the CSV file")
    
    # Check for None values in the grant_id column and replace them
    # Update to match 'None' values with single quotes included
    mask = df['grant_id'] == "'None'"
    none_count = mask.sum()
    print(f"Found {none_count} rows with 'None' values in grant_id column")
    
    if none_count > 0:
        # Display sample of rows that will be modified
        print("\nSample of rows to be modified:")
        sample_rows = df[mask].head(3)
        for _, row in sample_rows.iterrows():
            print(f"  Before: {row['nodeId']}, grant_id: {row['grant_id']}")
            print(f"  After:  {row['nodeId']}, grant_id: {row['country']}_{row['agency']}\n")
        
        # Replace 'None' values with country_agency
        df.loc[mask, 'grant_id'] = df.loc[mask, 'country'] + '_' + df.loc[mask, 'agency']
        print(f"Replaced {none_count} 'None' value(s) in grant_id column")
    else:
        print("No 'None' values found in grant_id column, no changes needed")
    
    # Save the modified dataframe to a new file
    df.to_csv(output_file, index=False)
    print(f"Processed file saved to: {output_file}")
    
    return none_count

if __name__ == "__main__":
    count = fill_none_grant_ids()
    print(f"\nSummary: Replaced {count} 'None' value(s) in grant_id column.")