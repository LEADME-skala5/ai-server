import json
import pandas as pd
from typing import Dict, List, Any

from pathlib import Path
from datetime import datetime
import logging
import openai
import os


# .env 파일 지원
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ .env 파일 로드 완료")
except ImportError:
    print("⚠️ python-dotenv 패키지가 설치되지 않음 - pip install python-dotenv")
    pass

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FinalReportAgent:
    def __init__(self, input_path: str = "./output", output_path: str = "./reports"):
        """
        개인별 평가 보고서 생성 에이전트 (JSON 형태로만 저장)
        """
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.output_path.mkdir(exist_ok=True)
        
        # OpenAI API 키 설정 (.env 파일에서 자동 로드)
        api_key = os.getenv("OPENAI_API_KEY")
        
        if not api_key:
            # .env 파일에서 수동으로 읽기 시도
            env_file = Path(".env")
            if env_file.exists():
                print("🔍 .env 파일에서 API 키 수동 검색 중...")
                try:
                    with open(env_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip()
                            if line.startswith('OPENAI_API_KEY='):
                                api_key = line.split('=', 1)[1].strip().strip('"').strip("'")
                                print("✅ .env 파일에서 API 키 발견")
                                break
                except Exception as e:
                    print(f"❌ .env 파일 읽기 실패: {e}")
            
            if not api_key:
                raise ValueError(
                    "OpenAI API 키를 찾을 수 없습니다.\n"
                    ".env 파일에 다음과 같이 설정하세요:\n"
                    "OPENAI_API_KEY=your-api-key-here"
                )
        
        # API 키 마스킹하여 표시
        masked_key = api_key[:10] + "*" * (len(api_key) - 15) + api_key[-5:] if len(api_key) > 15 else api_key[:5] + "*" * (len(api_key) - 5)
        print(f"🔑 OpenAI API 키 로드 완료: {masked_key}")
        
        self.client = openai.OpenAI(api_key=api_key)
        self.evaluation_data = {}
        
    def load_evaluation_results(self) -> None:
        """JSON 평가 결과 파일들을 로드합니다."""
        json_files = list(self.input_path.glob("evaluation_EMP*.json"))
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                emp_num = data['employee_summary']['basic_info']['employee_number']
                self.evaluation_data[emp_num] = data
                
            except Exception as e:
                logger.error(f"파일 로드 실패 {json_file}: {str(e)}")
        
        print(f"✅ {len(self.evaluation_data)}명의 평가 데이터 로드 완료")
    
    def generate_individual_reports(self) -> List[str]:
        """각 직원별 개인 평가 보고서를 JSON 형태로 생성합니다."""
        
        if not self.evaluation_data:
            raise ValueError("평가 데이터가 로드되지 않았습니다.")
        
        report_files = []
        
        for emp_num, data in self.evaluation_data.items():
            emp_summary = data['employee_summary']
            basic_info = emp_summary['basic_info']
            
            print(f"\n📝 {basic_info['name']} ({emp_num}) JSON 보고서 생성 중...")
            
            # JSON 형태 보고서 생성
            report_json = self._create_individual_report_json(emp_num, emp_summary)
            
            # JSON 파일 저장
            json_output_file = self.output_path / f"individual_report_{emp_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(json_output_file, 'w', encoding='utf-8') as f:
                json.dump(report_json, f, ensure_ascii=False, indent=2)
            
            report_files.append(str(json_output_file))
            
            # JSON 내용을 콘솔에 출력
            print(f"✅ {basic_info['name']} JSON 보고서 생성 완료")
            print(f"📄 파일: {json_output_file.name}")
            print("\n📊 생성된 JSON 보고서 내용:")
            print(json.dumps(report_json, ensure_ascii=False, indent=2))
            print("=" * 80)
        
        return report_files
    
    def _create_individual_report_json(self, emp_num: str, emp_summary: Dict) -> Dict:
        """개인별 JSON 형태 보고서를 생성합니다."""
        
        basic_info = emp_summary['basic_info']
        activity_categorization = emp_summary['activity_categorization']
        pattern_analysis = emp_summary.get('performance_pattern_analysis', {})
        
        # 모든 팀 목표 정의
        all_team_goals = [
            "oud Professional 업무 진행 통한 BR/UR 개선",
            "CSP 파트너쉽 강화 통한 원가개선", 
            "oud 마케팅 및 홍보 통한 대외 oud 고객확보",
            "글로벌 사업 Tech-presales 진행"
        ]
        
        # 팀 목표별 데이터 매핑
        team_goals_data = []
        for goal in all_team_goals:
            goal_info = {
                "goal_name": goal,
                "content": "해당 없음",
                "assigned": "미배정",
                "contribution": "0건 활동"
            }
            
            # 해당 목표에 대한 활동 찾기
            for category in activity_categorization:
                if category['category'] == goal:
                    activities_text = ', '.join(category['activities'][:2]) + ('...' if len(category['activities']) > 2 else '') if category['activities'] else '해당 없음'
                    goal_info = {
                        "goal_name": goal,
                        "content": activities_text,
                        "assigned": "배정" if category['count'] > 0 else "미배정",
                        "contribution": f"{category['count']}건 활동"
                    }
                    break
            
            team_goals_data.append(goal_info)
        
        # AI로 분기 성과요약 생성
        quarterly_summary_text = self._generate_ai_quarterly_summary_text_only(basic_info, activity_categorization, pattern_analysis)
        
        # AI로 총합 평가 생성
        overall_evaluation_data = self._generate_ai_overall_evaluation_json(basic_info, activity_categorization, pattern_analysis)
        
        # JSON 구조 생성
        report_json = {
            "employee_basic_info": {
                "name": basic_info['name'],
                "employee_number": basic_info['employee_number'],
                "evaluation_period": basic_info['period'],
                "total_activities": basic_info['total_activities'],
                "report_generated_date": datetime.now().strftime('%Y년 %m월 %d일 %H:%M')
            },
            "team_goals_contribution": team_goals_data,
            "quarterly_performance_summary": {
                "summary_text": quarterly_summary_text
            },
            "overall_evaluation": {
                "grade": overall_evaluation_data['grade'],
                "evaluation_basis": overall_evaluation_data['evaluation_basis'],
                "quantitative_evidence": overall_evaluation_data['quantitative_evidence'],
                "qualitative_assessment": overall_evaluation_data['qualitative_assessment'],
                "future_development_directions": overall_evaluation_data['future_development_directions']
            }
        }
        
        return report_json
    
    def _generate_ai_quarterly_summary_text_only(self, basic_info: Dict, activity_categorization: List, pattern_analysis: Dict) -> str:
        """AI를 사용하여 분기 성과요약 텍스트만 생성합니다."""
        
        # 데이터 준비
        employee_data = {
            'name': basic_info['name'],
            'period': basic_info['period'],
            'total_activities': basic_info['total_activities'],
            'activity_categorization': activity_categorization,
            'strengths': pattern_analysis.get('strengths', []),
            'improvements': pattern_analysis.get('improvements', [])
        }
        
        prompt = f"""
다음 직원의 분기 성과 데이터를 분석하여 전문적인 성과요약 텍스트를 작성해주세요.

직원 정보:
- 이름: {employee_data['name']}
- 평가 기간: {employee_data['period']}
- 총 활동 수: {employee_data['total_activities']}건

활동 현황:
"""
        
        for category in activity_categorization:
            prompt += f"- {category['category']}: {category['count']}건 활동\n"
            if category['activities']:
                prompt += f"  주요 활동: {', '.join(category['activities'][:2])}\n"
            prompt += f"  기여도: {category['impact']}\n"
        
        prompt += f"""

강점: {', '.join(employee_data['strengths'])}
개선점: {', '.join(employee_data['improvements'])}

요구사항:
1. 위 데이터를 바탕으로 분기 성과요약을 작성해주세요
2. 객관적이고 전문적인 톤으로 작성
3. 구체적인 수치와 성과를 포함
4. 200-300자 정도의 분량
5. 다음 형식으로 시작: "{employee_data['name']} 직원은 {employee_data['period']} 기간 동안..."

분기 성과요약 텍스트만 작성해주세요:
        """
        
        try:
            print("🤖 AI 분기 성과요약 생성 중...")
            response = self.client.chat.completions.create(
                model="gpt-4-turbo",
                messages=[
                    {"role": "system", "content": "당신은 전문적인 HR 평가 전문가입니다. 객관적이고 구체적인 성과 평가 텍스트를 작성해주세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=500
            )
            
            ai_summary = response.choices[0].message.content.strip()
            print("✅ AI 분기 성과요약 생성 완료")
            
            return ai_summary
        
        except Exception as e:
            logger.error(f"AI 성과요약 생성 실패: {str(e)}")
            print(f"⚠️ AI 성과요약 생성 실패, 기본 텍스트 사용")
            # fallback
            active_goals = len([cat for cat in activity_categorization if cat['count'] > 0])
            return f"{basic_info['name']} 직원은 {basic_info['period']} 기간 동안 총 {basic_info['total_activities']}건의 업무 활동을 수행하였으며, {active_goals}개의 팀 목표에 참여하여 활발한 업무 활동을 전개했습니다."
    
    def _generate_ai_overall_evaluation_json(self, basic_info: Dict, activity_categorization: List, pattern_analysis: Dict) -> Dict:
        """AI를 사용하여 총합 평가를 JSON 형태로 생성합니다."""
        
        # 기본 통계 계산
        total_activities = sum(cat['count'] for cat in activity_categorization)
        active_goals_count = len([cat for cat in activity_categorization if cat['count'] > 0])
        goal_coverage = active_goals_count / 4 * 100
        
        # 등급 결정
        if total_activities >= 15 and active_goals_count >= 3:
            grade = "A (우수)"
        elif total_activities >= 10 and active_goals_count >= 2:
            grade = "B+ (양호)"
        elif total_activities >= 5:
            grade = "B (보통)"
        else:
            grade = "C (개선필요)"
        
        prompt = f"""
다음 직원의 평가 데이터를 바탕으로 총합 평가를 구조화하여 작성해주세요.

기본 정보:
- 이름: {basic_info['name']}
- 총 활동: {total_activities}건
- 참여 목표: {active_goals_count}/4개 ({goal_coverage:.0f}% 커버리지)
- 평가 등급: {grade}

활동 상세:
"""
        
        for category in activity_categorization:
            prompt += f"- {category['category']}: {category['count']}건\n"
        
        prompt += f"""

강점: {', '.join(pattern_analysis.get('strengths', []))}
개선점: {', '.join(pattern_analysis.get('improvements', []))}
업무 스타일: {pattern_analysis.get('work_style', '데이터 없음')}

요구사항:
다음 JSON 형식으로 응답해주세요:

{{
  "evaluation_basis": "평가 근거 내용 (2-3문장)",
  "quantitative_evidence": [
    "총 수행 활동: {total_activities}건 (목표 대비 평가)",
    "목표 참여도: {active_goals_count}/4개 목표 참여 ({goal_coverage:.0f}% 커버리지)",
    "추가 정량적 근거 1",
    "추가 정량적 근거 2"
  ],
  "qualitative_assessment": {{
    "strengths": "강점 평가 내용",
    "improvements": "개선점 평가 내용",
    "work_style": "업무 스타일 평가 내용"
  }},
  "future_development_directions": [
    "발전 방향 1",
    "발전 방향 2"
  ]
}}

JSON 형태로만 응답해주세요:
        """
        
        try:
            print("🤖 AI 총합 평가 생성 중...")
            response = self.client.chat.completions.create(
                model="gpt-4-turbo",
                messages=[
                    {"role": "system", "content": "당신은 전문적인 HR 평가 전문가입니다. 요청된 JSON 형식으로만 응답해주세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=800
            )
            
            ai_response = response.choices[0].message.content.strip()
            print("✅ AI 총합 평가 생성 완료")
            
            # JSON 파싱 시도
            try:
                # ```json 블록이 있으면 추출
                if "```json" in ai_response:
                    json_start = ai_response.find("```json") + 7
                    json_end = ai_response.find("```", json_start)
                    ai_response = ai_response[json_start:json_end].strip()
                
                ai_evaluation_data = json.loads(ai_response)
                ai_evaluation_data['grade'] = grade
                return ai_evaluation_data
                
            except json.JSONDecodeError:
                print("⚠️ AI JSON 파싱 실패, 기본 구조 사용")
                return self._get_fallback_evaluation_json(grade, total_activities, active_goals_count, goal_coverage, pattern_analysis)
        
        except Exception as e:
            logger.error(f"AI 총합 평가 생성 실패: {str(e)}")
            print(f"⚠️ AI 총합 평가 생성 실패, 기본 구조 사용")
            return self._get_fallback_evaluation_json(grade, total_activities, active_goals_count, goal_coverage, pattern_analysis)
    
    def _get_fallback_evaluation_json(self, grade: str, total_activities: int, active_goals_count: int, goal_coverage: float, pattern_analysis: Dict) -> Dict:
        """AI 실패 시 fallback JSON 구조"""
        
        if total_activities >= 12:
            evaluation_basis = "다양한 목표 영역에서 활발한 활동을 보이며 팀 목표 달성에 기여하고 있습니다."
        elif total_activities >= 8:
            evaluation_basis = "주요 목표에서 꾸준한 활동을 보이며 지속적인 발전 가능성을 확인할 수 있습니다."
        else:
            evaluation_basis = "일부 목표에서 활동을 보이나 전반적인 참여 확대가 필요한 상황입니다."
        
        return {
            "grade": grade,
            "evaluation_basis": evaluation_basis,
            "quantitative_evidence": [
                f"총 수행 활동: {total_activities}건 (목표 대비 {'충족' if total_activities >= 10 else '부족'})",
                f"목표 참여도: {active_goals_count}/4개 목표 참여 ({goal_coverage:.0f}% 커버리지)",
                f"활발한 영역: {len([1 for i in range(active_goals_count) if total_activities/active_goals_count >= 3])}개 목표에서 3건 이상 활동",
                f"집중도: 목표당 평균 {total_activities/4:.1f}건 활동"
            ],
            "qualitative_assessment": {
                "strengths": ', '.join(pattern_analysis.get('strengths', ['데이터 없음'])),
                "improvements": ', '.join(pattern_analysis.get('improvements', ['데이터 없음'])),
                "work_style": pattern_analysis.get('work_style', '데이터 없음')
            },
            "future_development_directions": [
                "현재 성과를 바탕으로 미참여 목표 영역 확장 검토",
                "지속적인 성과 개선을 위한 체계적 접근 필요"
            ]
        }


def main():
    """메인 실행 함수"""
    print("
          
          
          AI 기반 개인별 평가 보고서 생성 (JSON 전용) ===")
    
    try:
        # 에이전트 초기화 (.env에서 API 키 자동 로드)
        agent = FinalReportAgent(
            input_path="./output",
            output_path="./reports"
        )
        
        # 평가 결과 로드
        agent.load_evaluation_results()
        
        # 개인별 보고서 생성 (JSON만)
        report_files = agent.generate_individual_reports()
        
        print(f"\n🎉 총 {len(report_files)}개의 AI 기반 JSON 보고서 생성 완료!")
        print("📁 생성된 파일들:")
        for file in report_files:
            print(f"   📄 {Path(file).name}")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        logger.error(f"보고서 생성 오류: {str(e)}")

if __name__ == "__main__":
    main()