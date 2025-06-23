import json
import pandas as pd
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime
import logging
import openai
import os

from weekly_evaluations import (
    get_average_grade,
    get_weighted_workload_score,
    calculate_final_score
)

try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ .env 파일 로드 완료")
except ImportError:
    print("⚠️ python-dotenv 패키지가 설치되지 않음 - pip install python-dotenv")
    pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeeklyReportAgent:
    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(".env 파일에 OPENAI_API_KEY가 없습니다.")

        masked_key = api_key[:10] + "*" * (len(api_key) - 15) + api_key[-5:]
        print(f"🔑 OpenAI API 키 로드 완료: {masked_key}")
        self.client = openai.OpenAI(api_key=api_key)

    def load_evaluation_data(self, input_path: str) -> Dict:
        with open(input_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def create_weekly_report_json(self, evaluation_data: Dict) -> Dict:
        emp_summary = evaluation_data['employee_summary']
        basic_info = emp_summary['basic_info']
        activity_categorization = emp_summary['activity_categorization']
        pattern_analysis = emp_summary.get('performance_pattern_analysis', {})

        employee_info = {
            "name": basic_info['name'],
            "department": basic_info.get('department', '클라우드 개발 3팀'),
            "period": basic_info['period']
        }

        all_team_goals = [
            "oud Professional 업무 진행 통한 BR/UR 개선",
            "CSP 파트너쉽 강화 통한 원가개선",
            "oud 마케팅 및 홍보 통한 대외 oud 고객확보",
            "글로벌 사업 Tech-presales 진행"
        ]

        team_goals_data = []
        key_achievements_data = []

        for goal in all_team_goals:
            matched = next((cat for cat in activity_categorization if cat['category'] == goal), None)
            if matched:
                count = matched.get('count', 0)
                activities = matched.get('activities', [])
                team_goals_data.append({
                    "goalName": goal,
                    "assigned": "배정" if count > 0 else "미배정",
                    "content": ", ".join(activities) if activities else "-",
                    "contributionCount": count
                })
                if count > 0:
                    key_achievements_data.append(f"{goal}: {count}건")
            else:
                team_goals_data.append({
                    "goalName": goal,
                    "assigned": "미배정",
                    "content": "-",
                    "contributionCount": 0
                })

        total_activities = basic_info['total_activities']
        active_goals = len([g for g in team_goals_data if g['contributionCount'] > 0])
        coverage = (active_goals / 4) * 100

        key_achievements_summary = [
            f"총 수행 활동: {total_activities}건 (목표 대비 평가)",
            f"목표 참여도: {active_goals}/4개 목표 참여 ({coverage:.0f}% 커버리지)"
        ]
        final_key_achievements = key_achievements_summary + key_achievements_data

        # ✅ 정량 평가 점수 계산
        employee_id = basic_info.get('employee_number')
        year = int(basic_info.get('year', datetime.now().year))
        quarter = int(basic_info.get('quarter', 1))

        try:
            avg_score = get_average_grade(employee_id, year, quarter)
            workload_score = get_weighted_workload_score(employee_id, year, quarter)
            final_score = calculate_final_score(avg_score, workload_score)
        except Exception as e:
            logger.warning(f"정량 점수 계산 실패: {e}")
            avg_score = workload_score = final_score = 0.0

        # ✅ AI 요약 생성
        quarterly_summary = self._generate_ai_quarterly_summary_text_only(
            basic_info, activity_categorization, pattern_analysis
        )

        return {
            "employee": employee_info,
            "teamGoals": team_goals_data,
            "keyAchievements": final_key_achievements,
            "quarterlyPerformanceSummary": quarterly_summary,
            "evaluationScore": {
                "averageScore": avg_score,
                "workloadScore": workload_score,
                "finalScore": final_score
            }
        }

    def _generate_ai_quarterly_summary_text_only(self, basic_info: Dict, activity_categorization: List, pattern_analysis: Dict) -> str:
        prompt = f"""
다음 직원의 분기 성과 데이터를 분석하여 전문적인 성과요약 텍스트를 작성해주세요.

직원 정보:
- 이름: {basic_info['name']}
- 평가 기간: {basic_info['period']}
- 총 활동 수: {basic_info['total_activities']}건

활동 현황:
"""
        for cat in activity_categorization:
            prompt += f"- {cat['category']}: {cat['count']}건\n"
            if cat.get('activities'):
                prompt += f"  주요 활동: {', '.join(cat['activities'][:2])}\n"
            prompt += f"  기여도: {cat.get('impact', '중간')}\n"

        prompt += f"""
강점: {', '.join(pattern_analysis.get('strengths', []))}
개선점: {', '.join(pattern_analysis.get('improvements', []))}

요구사항:
- 전문적인 성과요약 작성 (200~300자)
- "{basic_info['name']} 직원은 {basic_info['period']} 기간 동안..." 으로 시작
"""
        try:
            res = self.client.chat.completions.create(
                model="gpt-4-turbo",
                messages=[
                    {"role": "system", "content": "당신은 HR 전문가입니다."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=500
            )
            return res.choices[0].message.content.strip()
        except Exception as e:
            logger.warning(f"AI 요약 실패: {e}")
            return f"{basic_info['name']} 직원은 {basic_info['period']} 기간 동안 총 {basic_info['total_activities']}건의 활동을 수행했습니다."


def generate_weekly_report(input_path: str, output_path: Optional[str] = None) -> Dict:
    agent = WeeklyReportAgent()
    data = agent.load_evaluation_data(input_path)
    report = agent.create_weekly_report_json(data)

    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ 보고서 저장 완료: {output_path}")

    return report


def main():
    input_dir = Path("./output")
    output_dir = Path("./reports")
    output_dir.mkdir(exist_ok=True)

    agent = WeeklyReportAgent()
    files = list(input_dir.glob("evaluation_EMP*.json"))

    if not files:
        print("❌ 평가 파일이 없습니다.")
        return

    for file in files:
        try:
            data = agent.load_evaluation_data(str(file))
            report = agent.create_weekly_report_json(data)

            emp_num = data['employee_summary']['basic_info']['employee_number']
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            out_path = output_dir / f"weekly_report_{emp_num}_{timestamp}.json"

            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)

            print(f"✅ {out_path.name} 저장 완료")
        except Exception as e:
            print(f"❌ {file.name} 처리 실패: {e}")


if __name__ == "__main__":
    main()
