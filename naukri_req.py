import os
import csv
import asyncio
import random
import time
from pyppeteer import launch
import stealth_requests as requests
import pandas as pd

async def fetch_cookies():
    browser = await launch(headless=True,
        executablePath='C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe',
        args=['--no-sandbox']
    )
    page = await browser.newPage()

    await page.goto('https://www.naukri.com/it-jobs?src=gnbjobs_homepage_srch')

    await page.waitForSelector('body')

    cookies = await page.cookies()

    await browser.close()

    return {cookie['name']: cookie['value'] for cookie in cookies}

def extract_placeholders(placeholders):
    """Extract experience, salary, and location from placeholders."""
    experience = ''
    salary = ''
    location = ''

    if isinstance(placeholders, list):
        for item in placeholders:
            if isinstance(item, dict):
                if item.get('type') == 'experience':
                    experience = item.get('label', '')
                elif item.get('type') == 'salary':
                    salary = item.get('label', '')
                elif item.get('type') == 'location':
                    location = item.get('label', '')

    return experience, salary, location

async def fetch_job_data(keyword, cookies_dict, page_no):
    """Fetch job data for a given keyword and page number."""
    url = f'https://www.naukri.com/jobapi/v3/search?noOfResults=20&urlType=search_by_keyword&searchType=adv&keyword={keyword}&pageNo={page_no}&seoKey=it-jobs&src=gnbjobs_homepage_srch&latLong='

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'appid': '109',
        'systemid': 'Naukri',
    }

    time.sleep(random.randrange(2, 4))

    response = requests.get(url, headers=headers, cookies=cookies_dict)

    print(f"Fetching jobs for keyword: {keyword}, page: {page_no} - Status Code: {response.status_code}")


    if response.status_code == 200:
        print("Success!")
        return response.json().get('jobDetails', [])
    else:
        print(f"Failed to fetch jobs for keyword '{keyword}' on page '{page_no}' with status code: {response.status_code}")
        return []

async def main():
    cookies_dict = await fetch_cookies()

    keywords = [
        'software', # 225
        'developer',
        'engineer',
        'programmer',
        'analyst',
        'software architect',
        'data scientist',
        'data analyst',
        'data engineer',
        'machine learning',
        'artificial intelligence',
        'cyber security',
        'cloud computing',
    ]

    desired_fieldnames = [
        'title',
        'companyName',
        'location',
        'experience',
        'salary',
        'currency',
        'tagsAndSkills',
        'jobDescription',
        'jdURL',
        'staticUrl',
        'jobId',
        'companyId',
        'isSaved',
        'vacancy',
        'groupId',
        'jobUploadDate',
        'createdDate'
    ]

    file_exists = os.path.isfile('job_details.csv')

    job_ids = set() # initialize a set to track job IDs

    if file_exists:
        df_existing = pd.read_csv('job_details.csv')
        job_ids.update(df_existing['jobId'].astype(str).tolist())

    with open('job_details.csv', mode='a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=desired_fieldnames)

        if not file_exists:
            writer.writeheader()

        total_jobs_written = 0
        total_jobs_skipped = 0

        for keyword in keywords:
            page_no = 225
            while True:
                job_details_list = await fetch_job_data(keyword, cookies_dict, page_no)

                if not job_details_list:
                    break

                for job_details in job_details_list:
                    job_id = job_details.get('jobId', '')

                    if job_id in job_ids:
                        total_jobs_skipped += 1
                        continue

                    experience, salary, location = extract_placeholders(job_details.get('placeholders', []))

                    filtered_job_details = {
                        'title': job_details.get('title', ''),
                        'companyName': job_details.get('companyName', ''),
                        'location': location,
                        'experience': experience,
                        'salary': salary,
                        'currency': job_details.get('currency', ''),
                        'tagsAndSkills': job_details.get('tagsAndSkills', ''),
                        'jobDescription': job_details.get('jobDescription', ''),
                        'jdURL': job_details.get('jdURL', ''),
                        'staticUrl': job_details.get('staticUrl', ''),
                        'jobId': job_id,
                        'companyId': job_details.get('companyId', ''),
                        'isSaved': job_details.get('isSaved', ''),
                        'vacancy': job_details.get('vacancy', ''),
                        'groupId': job_details.get('groupId', ''),
                        'jobUploadDate': job_details.get('footerPlaceholderLabel', ''),
                        'createdDate': job_details.get('createdDate', ''),
                    }

                    writer.writerow(filtered_job_details)
                    job_ids.add(job_id)
                    total_jobs_written += 1
                    print(f"Added Job: {job_details.get('title', '')}")

                csvfile.flush()

                df = pd.read_csv('job_details.csv')
                print(f"Total jobs written so far: {len(df)}")
                print(f"Total jobs skipped so far: {total_jobs_skipped}\n")

                page_no += 1

    print("Job details saved to 'job_details.csv'.")

asyncio.get_event_loop().run_until_complete(main())