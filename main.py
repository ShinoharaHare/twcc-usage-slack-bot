import json
import subprocess
import time
from itertools import groupby
from typing import Any, Callable, Dict, List

import fire
import schedule
from slack_sdk import WebClient

Config = Dict[str, Any]
Job = Dict[str, str]

def read_json(path: str) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def build_command(config: Config) -> List[str]:
    account_list = ','.join(config['project_map'].keys())
    return ['squeue', '-a', '-o', '%all', '-A', account_list]

def parse_squeue_output(output: str) -> Job:
    lines = output.strip().split('\n')
    header = lines[0].split('|')

    jobs = []
    for l in lines[1:]:
        job = {k: v for k, v in zip(header, l.split('|'))}
        jobs.append(job)
    return jobs

def get_formatters(config: Dict[str, Any]) -> List[Callable[[Job], str]]:
    def get_simple_formmatter(display_name: str, field_name: str):
        return lambda j: f'{display_name}：{j[field_name]}'

    def project_formatter(job: Job):
        project_map = {k.upper(): v for k, v in config['project_map'].items()} .copy()
        x = job['ACCOUNT'].upper()
        if x in project_map:
            x = f'{x}({project_map[x]})'
        x = f'📜 計畫ID：{x}'
        return x
    
    def user_formatter(job: Job):
        user_map = config['user_map']
        x = job['USER']
        if x in user_map:
            x = f'{x}({user_map[x]})'
        x = f'🤪 使用者ID：{x}'
        return x

    formatters = [
        get_simple_formmatter('🪪 任務ID', 'JOBID'),
        get_simple_formmatter('🛠️ 任務名稱', 'NAME'),
        get_simple_formmatter('⚓ 分區名稱', 'PARTITION'),
        get_simple_formmatter('🖥️ 節點數量', 'NODES'),
        get_simple_formmatter('⏱ 開始時間', 'START_TIME'),
        get_simple_formmatter('⏳ 運行時間', 'TIME'),
        project_formatter,
        user_formatter,
    ]

    return formatters


def get_blocks(jobs: Dict[str, str], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    user_map = config['user_map']
    formatters = get_formatters(config)

    blocks = [
        {
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": f"📋 TWCC HPC 任務例行檢查",
				"emoji": True
			}
		},
        {
            "type": "section",
			"text": {
				"type": "mrkdwn",
				"text": f"⌚ 檢查時間： *{time.strftime('%Y/%m/%d %H:%M:%S')}*"
            }
        },
        {"type": "divider"}
    ]

    for user, job_group in groupby(jobs, key=lambda x: x['USER']):
        if user in user_map:
            user += f'({user_map[user]})'
        
        blocks.append({
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": f"🤪 *{user}* 有以下任務正在運行："
            }
		})
        
        for job in job_group:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"🛠️ *{job['NAME'] or '_未命名_'}*"
                }
            })
            
            blocks.append({
                "type": "section",
                "fields": [
                    {
                        "type": "plain_text",
                        "text": formatter(job),
                        "emoji": True
                    } for formatter in formatters
                ]
		    })
        blocks.append({"type": "divider"})

    return blocks

def main(
    config_path: str = 'config.json'
):
    config = read_json(config_path)
    client = WebClient(token=config['slack']['token'])
    command = build_command(config)

    def routine():
        process = subprocess.run(command, stdout=subprocess.PIPE)
        jobs = parse_squeue_output(process.stdout.decode())
        client.chat_postMessage(
            channel=config['slack']['channel'],
            text='📋 TWCC HPC 任務例行檢查',
            blocks=get_blocks(jobs, config)
        )

    routine()
    schedule.every().day.at('00:00').do(routine)
    schedule.every().day.at('12:00').do(routine)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    fire.Fire(main)
