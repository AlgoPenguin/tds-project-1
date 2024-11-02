import requests
import pandas as pd
import time
import os
import sys



# GitHub API base URL
GITHUB_API_URL = 'https://api.github.com'

# Number of users per page (maximum 100)
USERS_PER_PAGE = 100

# Number of repositories per page (maximum 100)
REPOS_PER_PAGE = 100

# Maximum number of pages to fetch for users (GitHub Search API caps at 1000 results)
MAX_USER_PAGES = 10

# Maximum number of repositories to fetch per user
MAX_REPOS_PER_USER = 500

# Output CSV file names
USERS_CSV = 'users.csv'
REPOSITORIES_CSV = 'repositories.csv'

# Helper Functions

def get_github_token():
    """
    Retrieve the GitHub Personal Access Token (PAT) from environment variables.
    Exits the script if the token is not found.
    """
    token = os.getenv('GITHUB_TOKEN')
    if not token:
        print("Error: GitHub Personal Access Token (PAT) not found in environment variables.")
        print("Please set the 'GITHUB_TOKEN' environment variable and try again.")
        sys.exit(1)
    return token.strip()

def clean_company(company):
    """
    Clean the 'company' field by trimming whitespace, stripping the leading '@',
    and converting to uppercase. Returns an empty string if company is None or empty.
    """
    if not company:
        return ''
    company = company.strip()
    if company.startswith('@'):
        company = company[1:]
    company = company.upper()
    return company

def handle_boolean(value):
    """
    Convert boolean values to lowercase strings 'true' or 'false'.
    Return an empty string if the value is None.
    """
    if isinstance(value, bool):
        return str(value).lower()
    return ''

def fetch_users(location, min_followers, headers, max_pages=10):
    """
    Fetch GitHub users based in the specified location with more than min_followers.
    Returns a list of user dictionaries.
    """
    users = []
    query = f'location:"{location}" followers:>{min_followers}'
    url = f'{GITHUB_API_URL}/search/users'
    params = {
        'q': query,
        'per_page': USERS_PER_PAGE,
        'page': 1
    }

    for page in range(1, max_pages + 1):
        params['page'] = page
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f'Error fetching users (Page {page}): {response.status_code} - {response.text}')
            break
        data = response.json()
        fetched_users = data.get('items', [])
        users.extend(fetched_users)
        print(f'Fetched page {page} with {len(fetched_users)} users.')
        if 'next' not in response.links:
            break
        time.sleep(1)  # Respect rate limits
    return users

def fetch_user_details(username, headers):
    """
    Fetch detailed information for a specific GitHub user.
    Returns a dictionary of user details.
    """
    url = f'{GITHUB_API_URL}/users/{username}'
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f'Error fetching details for {username}: {response.status_code} - {response.text}')
        return {}
    return response.json()

def fetch_repositories(username, headers, max_repos=500):
    """
    Fetch repositories for a specific GitHub user.
    Returns a list of repository dictionaries.
    """
    repos = []
    url = f'{GITHUB_API_URL}/users/{username}/repos'
    params = {
        'per_page': REPOS_PER_PAGE,
        'page': 1,
        'sort': 'pushed',
        'direction': 'desc'
    }

    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f'Error fetching repos for {username}: {response.status_code} - {response.text}')
            break
        data = response.json()
        if not data:
            break
        repos.extend(data)
        if len(repos) >= max_repos or 'next' not in response.links:
            break
        params['page'] += 1
        time.sleep(1)  # Respect rate limits
    return repos[:max_repos]

# Main 

def main():
    # Step 1: Get GitHub Token from Environment Variable
    GITHUB_TOKEN = get_github_token()

    headers = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json'
    }

    # Step 2: Fetch Users
    print("\nStarting to fetch users...")
    users = fetch_users(location='Tokyo', min_followers=200, headers=headers, max_pages=MAX_USER_PAGES)
    print(f'\nTotal users fetched: {len(users)}')

    # Step 3: Process Users and Fetch Details
    print("\nProcessing users and fetching detailed information...")
    processed_users = []
    all_repositories = []

    for idx, user in enumerate(users, start=1):
        username = user.get('login')
        print(f'\nProcessing user {idx}/{len(users)}: {username}')

        # Fetch user details
        details = fetch_user_details(username, headers)
        if not details:
            print(f'Skipping user {username} due to error in fetching details.')
            continue

        # Clean and process user data
        user_data = {
            'login': details.get('login', ''),
            'name': details.get('name') or '',
            'company': clean_company(details.get('company')),
            'location': details.get('location') or '',
            'email': details.get('email') or '',
            'hireable': handle_boolean(details.get('hireable')),
            'bio': details.get('bio') or '',
            'public_repos': details.get('public_repos', 0),
            'followers': details.get('followers', 0),
            'following': details.get('following', 0),
            'created_at': details.get('created_at', '')
        }
        processed_users.append(user_data)

        # Fetch repositories for the user
        repos = fetch_repositories(username, headers, max_repos=MAX_REPOS_PER_USER)
        print(f'Fetched {len(repos)} repositories for user {username}.')

        for repo in repos:
            repo_data = {
                'login': username,
                'full_name': repo.get('full_name', ''),
                'created_at': repo.get('created_at', ''),
                'stargazers_count': repo.get('stargazers_count', 0),
                'watchers_count': repo.get('watchers_count', 0),
                'language': repo.get('language') or '',
                'has_projects': handle_boolean(repo.get('has_projects')),
                'has_wiki': handle_boolean(repo.get('has_wiki')),
                'license_name': repo['license']['key'] if repo.get('license') else ''
            }
            all_repositories.append(repo_data)

        # Optional: Print progress every 10 users
        if idx % 10 == 0:
            print(f'Processed {idx} users out of {len(users)}.')

    # Step 4: Create DataFrames
    print("\nCreating DataFrames for users and repositories...")
    users_df = pd.DataFrame(processed_users)
    repos_df = pd.DataFrame(all_repositories)

    # Step 5: Save to CSV
    print(f"\nSaving users data to {USERS_CSV}...")
    users_df.to_csv(USERS_CSV, index=False)
    print(f"Saved {USERS_CSV} successfully.")

    print(f"\nSaving repositories data to {REPOSITORIES_CSV}...")
    repos_df.to_csv(REPOSITORIES_CSV, index=False)
    print(f"Saved {REPOSITORIES_CSV} successfully.")

    print("\nData collection and processing completed successfully!")

if __name__ == "__main__":
    main()
