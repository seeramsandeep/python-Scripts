# Task API

# import requests
# import pandas as pd
# import json

# # Initialize Elasticsearch client
# baseURL = 'https://pda.socion.io/taskservice/api/v2/reports/ext/submission-data'
# programId = "200"
# clientSecretKeyId = "726543"
# clientSecretKey = "cde690b6-7223-4676-adda-b27f672c6a7c"
# taskId = "020a52a8-0271-4a5f-8afc-a3bf2817e5e4"

# url = baseURL + '?programId=' + programId + '&client_secret_key_id=' + clientSecretKeyId + '&client_secret_key=' + clientSecretKey + '&taskId=' + taskId

# print(url)

# headers = {
#     'Content-Type': 'application/json'
# }

# response = requests.get(url, headers=headers)


# dfs=[]
# if response.status_code == 200:
   
#     # Parse and print the response content
#     resp = response.json()
#     scrollID = resp["scrollId"]
#     df = pd.DataFrame(resp["result"]["data"])
#     df['Response'] = df['Response'].apply(json.dumps)
#     dfs.append(df)
   
#     print(scrollID)
   
   
#     while(scrollID != ""):
#          updatedUrl = url + "&scrollId=" + scrollID
#          print("Updated URL: " + updatedUrl)
#          response1 = requests.get(updatedUrl, headers=headers)
#          if response1.status_code == 200:
#              resp1 = response1.json()
#              df1 = pd.DataFrame(resp1["result"]["data"])
#              df1['Response'] = df1['Response'].apply(json.dumps)
#              scrollID = resp1["scrollId"]
#              print("internal scroll: " + scrollID)
#              dfs.append(df1)
#              print(len(dfs))
#     finalDF = pd.concat(dfs)
#     print(finalDF)

# else:
#     print("Request failed with status code: {response.status_code}")

















#  QP API

import requests
import pandas as pd
import json

# Initialize parameters for qp-completion-progress
baseURL = 'https://pda.socion.io/reports/ext/v2/qp-completion-progress'
programId = "80"
clientSecretKeyId = "100"
clientSecretKey = "b58ef8c0-fcc0-4dbb-aba4-74462fa8717a"

url = baseURL + '?programId=' + programId + '&client_secret_key_id=' + clientSecretKeyId + '&client_secret_key=' + clientSecretKey

print(url)

headers = {
    'Content-Type': 'application/json'
}

response = requests.get(url, headers=headers)

dfs = []
if response.status_code == 200:
    resp = response.json()
    scrollID = resp["scrollId"]
    
    df = pd.DataFrame(resp["response"])
    print("Column names:", df.columns)

    # Checking if 'qpDetails' column exists
    if 'qpDetails' in df.columns:
        df['qpDetails'] = df['qpDetails'].apply(json.dumps)

    if not df.empty:  # Add only if the DataFrame is not empty
        dfs.append(df)
    
    print(scrollID)
    
    # Fetch additional pages if scrollID is available
    while scrollID != "":
        updatedUrl = url + "&scrollId=" + scrollID
        print("Updated URL: " + updatedUrl)
        response1 = requests.get(updatedUrl, headers=headers)
        if response1.status_code == 200:
            resp1 = response1.json()

            # Check if the response data is empty, stop if it's empty
            if not resp1["response"]:
                print("No more records to fetch.")
                break

            df1 = pd.DataFrame(resp1["response"])

            # Check if 'qpDetails' column exists before applying json.dumps
            if 'qpDetails' in df1.columns:
                df1['qpDetails'] = df1['qpDetails'].apply(json.dumps)

            scrollID = resp1.get("scrollId", "")
            print("Internal scroll ID: " + scrollID)

            if not scrollID:
                break

            if not df1.empty:
                dfs.append(df1)

            print(len(dfs))
        else:
            print(f"Request failed with status code: {response1.status_code}")
            break

    # Concatenate all DataFrames
    if dfs:
        finalDF = pd.concat(dfs)
        print(finalDF)
        print(f"Final DataFrame shape: {finalDF.shape}")
    else:
        print("No records found.")
else:
    print(f"Request failed with status code: {response.status_code}")
