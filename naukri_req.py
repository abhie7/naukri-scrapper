import os
import csv
import asyncio
import random
import time
from pyppeteer import launch
import requests

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

async def fetch_job_data(keyword, cookies_dict):
    """Fetch job data for a given keyword."""
    url = f'https://www.naukri.com/jobapi/v3/search?noOfResults=100&urlType=search_by_keyword&searchType=adv&keyword={keyword}&pageNo=1&seoKey=it-jobs&src=gnbjobs_homepage_srch&latLong='

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'appid': '109',
        'systemid': 'Naukri',
    }

    response = requests.get(url, headers=headers, cookies=cookies_dict)

    print(f"Fetching jobs for keyword: {keyword} - Status Code: {response.status_code}")

    time.sleep(random.randrange(3,9))

    if response.status_code == 200:
        print("Success!")
        return response.json().get('jobDetails', [])
    else:
        print(f"Failed to fetch jobs for keyword '{keyword}' with status code: {response.status_code}")
        return []

async def main():
    cookies_dict = await fetch_cookies()

    keywords = [
        'software engineer',
        'data scientist',
        'devops engineer',
        'full stack developer',
        'front end developer',
        'back end developer',
        'machine learning engineer',
        'web developer',
        'QA engineer',
        'IT support',
        'cloud engineer',
        'network engineer',
        'cybersecurity',
        'mobile developer',
        'business analyst',
        'database administrator',
        'systems analyst',
        'IT project manager',
        'technical writer',
        'UI/UX designer',
        'site reliability engineer',
        'big data engineer',
        'blockchain developer',
        'AI engineer',
        'robotics engineer',
        'data engineer',
        'game developer',
        'IT consultant',
        'product manager',
        'salesforce developer',
        'network architect',
        'information security analyst',
        'application support analyst',
        'IT operations manager',
        'computer programmer',
        'solutions architect',
        'test automation engineer',
        'digital marketing specialist',
        'e-commerce developer',
        'content management system (CMS) developer',
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

    with open('job_details.csv', mode='a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=desired_fieldnames)

        if not file_exists:
            writer.writeheader()

        for keyword in keywords:
            job_details_list = await fetch_job_data(keyword, cookies_dict)

            for job_details in job_details_list:
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
                    'jobId': job_details.get('jobId', ''),
                    'companyId': job_details.get('companyId', ''),
                    'isSaved': job_details.get('isSaved', ''),
                    'vacancy': job_details.get('vacancy', ''),
                    'groupId': job_details.get('groupId', ''),
                    'jobUploadDate': job_details.get('footerPlaceholderLabel', ''),
                    'createdDate': job_details.get('createdDate', ''),
                }

                writer.writerow(filtered_job_details)

    print("Job details saved to 'job_details.csv'.")

asyncio.get_event_loop().run_until_complete(main())