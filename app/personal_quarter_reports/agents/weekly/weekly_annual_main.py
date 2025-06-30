#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
연말 정량 평가 시스템
MongoDB에서 분기별 데이터를 수집하여 AI 기반 연말 종합 평가 생성
"""

import pymongo
import json
import openai
from typing import Dict, List, Any, Optional
import os
from datetime import datetime
from pathlib import Path
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =====================================
# 설정 클래스
class Config:
    def __init__(self):
        self.OPENAI_API_KEY = "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"
        self.MODEL = "gpt-4-turbo"
        self.OUTPUT_PATH = "./annual_output"
        
        # MongoDB 설정
        self.MONGODB_CONFIG = {
            'host': 'root',  # MongoDB 호스트
            'port': 27017,        # MongoDB 포트
            'database': 'skala',  # 데이터베이스 이름
            'collection': 'weekly_evaluation_results'  # 컬렉션 이름
        }

# =====================================
# MongoDB 관리자
class MongoDBManager:
    def __init__(self, mongo_config):
        self.config = mongo_config
        self.client = None
        self.db = None
        self.collection = None
        
    def connect(self):
        """MongoDB 연결"""
        try:
            connection_string = f"mongodb://{self.config['host']}:{self.config['port']}/"
            self.client = pymongo.MongoClient(connection_string)
            self.db = self.client[self.config['database']]
            self.collection = self.db[self.config['collection']]
            
            # 연결 테스트
            self.client.server_info()
            logger.info("MongoDB 연결 성공")
            return True
            
        except Exception as e:
            logger.error(f"MongoDB 연결 실패: {str(e)}")
            return False
    
    def get_user_quarterly_data(self, user_id, year=2024):
        """특정 사용자의 연간 분기별 데이터 조회"""
        try:
            logger.info(f"사용자 {user_id}의 {year}년 분기별 데이터 조회 시작")
            
            # 쿼리 조건
            query = {
                "user.userId": int(user_id),
                "evaluated_year": year,
                "type": "personal-quarter"
            }
            
            # 분기순 정렬
            sort_criteria = [("evaluated_quarter", 1)]
            
            results = list(self.collection.find(query).sort(sort_criteria))
            
            logger.info(f"조회 결과: {len(results)}개 분기 데이터")
            
            if not results:
                logger.warning(f"사용자 {user_id}의 {year}년 데이터가 없습니다.")
                return None
            
            # 분기별로 정리
            quarterly_data = {}
            for doc in results:
                quarter = doc.get('evaluated_quarter')
                if quarter:
                    quarterly_data[f"Q{quarter}"] = doc
            
            return quarterly_data
            
        except Exception as e:
            logger.error(f"데이터 조회 실패: {e}")
            return None
    
    def get_available_users(self, year=2024):
        """해당 연도에 데이터가 있는 사용자 목록 조회"""
        try:
            pipeline = [
                {
                    "$match": {
                        "evaluated_year": year,
                        "type": "personal-quarter"
                    }
                },
                {
                    "$group": {
                        "_id": "$user.userId",
                        "name": {"$first": "$user.name"},
                        "department": {"$first": "$user.department"},
                        "quarter_count": {"$sum": 1}
                    }
                },
                {
                    "$sort": {"_id": 1}
                }
            ]
            
            results = list(self.collection.aggregate(pipeline))
            
            users = []
            for result in results:
                users.append({
                    "user_id": result["_id"],
                    "name": result.get("name", f"User_{result['_id']}"),
                    "department": result.get("department", "미지정"),
                    "available_quarters": result["quarter_count"]
                })
            
            logger.info(f"{year}년 데이터가 있는 사용자: {len(users)}명")
            return users
            
        except Exception as e:
            logger.error(f"사용자 목록 조회 실패: {e}")
            return []
    
    def close(self):
        """MongoDB 연결 종료"""
        if self.client:
            self.client.close()
            logger.info("MongoDB 연결 종료")

# =====================================
# 데이터 분석기
class QuarterlyDataAnalyzer:
    @staticmethod
    def analyze_quarterly_data(quarterly_data):
        """분기별 데이터 분석"""
        if not quarterly_data:
            return None
        
        analysis = {
            "available_quarters": list(quarterly_data.keys()),
            "total_quarters": len(quarterly_data),
            "user_info": None,
            "goals_summary": {},
            "performance_trend": [],
            "consistency_analysis": {}
        }
        
        # 사용자 정보 추출 (첫 번째 분기에서)
        first_quarter = list(quarterly_data.values())[0]
        analysis["user_info"] = first_quarter.get("user", {})
        
        # 분기별 목표 분석
        all_goals = set()
        quarter_goals = {}
        
        for quarter, data in quarterly_data.items():
            team_goals = data.get("teamGoals", [])
            quarter_goals[quarter] = []
            
            for goal in team_goals:
                goal_name = goal.get("goalName", "")
                contribution_count = goal.get("contributionCount", 0)
                assigned = goal.get("assigned", "미배정")
                
                if goal_name:
                    all_goals.add(goal_name)
                    quarter_goals[quarter].append({
                        "name": goal_name,
                        "contribution": contribution_count,
                        "assigned": assigned,
                        "contents": goal.get("contents", [])
                    })
        
        analysis["goals_summary"] = {
            "total_unique_goals": len(all_goals),
            "quarterly_breakdown": quarter_goals
        }
        
        # 일관성 분석
        goal_frequency = {}
        for goal in all_goals:
            frequency = 0
            for quarter_data in quarter_goals.values():
                if any(g["name"] == goal for g in quarter_data):
                    frequency += 1
            goal_frequency[goal] = frequency
        
        # 4분기 모두 참여한 목표
        consistent_goals = [goal for goal, freq in goal_frequency.items() if freq == 4]
        
        analysis["consistency_analysis"] = {
            "consistent_goals": consistent_goals,
            "goal_frequency": goal_frequency,
            "consistency_rate": len(consistent_goals) / len(all_goals) if all_goals else 0
        }
        
        return analysis

# =====================================
# AI 평가기
class AnnualAIEvaluator:
    def __init__(self, api_key, model="gpt-4-turbo"):
        self.client = openai.OpenAI(api_key=api_key)
        self.model = model
    
    def generate_annual_prompt(self, quarterly_data, analysis):
        """연말 평가용 프롬프트 생성"""
        
        user_info = analysis["user_info"]
        user_name = user_info.get("name", "사용자")
        department = user_info.get("department", "미지정 부서")
        
        prompt = f"""
당신은 전문적인 HR 연말 평가 에이전트입니다. 직원의 4분기 성과 데이터를 종합 분석하여 연말 정량 평가를 수행해주세요.

## 평가 대상 정보
- 이름: {user_name}
- 부서: {department}
- 평가 연도: 2024년
- 분석 대상 분기: {', '.join(analysis['available_quarters'])}

## 분기별 상세 성과 데이터

"""
        
        # 분기별 데이터 추가
        for quarter, data in quarterly_data.items():
            prompt += f"### {quarter} 성과\n"
            team_goals = data.get("teamGoals", [])
            
            if team_goals:
                for goal in team_goals:
                    goal_name = goal.get("goalName", "")
                    contribution = goal.get("contributionCount", 0)
                    assigned = goal.get("assigned", "")
                    contents = goal.get("contents", [])
                    
                    prompt += f"**목표**: {goal_name}\n"
                    prompt += f"**기여도**: {contribution}건\n"
                    prompt += f"**배정상태**: {assigned}\n"
                    
                    if contents:
                        prompt += f"**주요 활동**:\n"
                        for content in contents[:3]:  # 상위 3개만
                            desc = content.get("description", "")
                            if desc:
                                prompt += f"- {desc}\n"
                    prompt += "\n"
            else:
                prompt += "해당 분기 데이터 없음\n"
            
            prompt += "\n---\n\n"
        
        prompt += f"""
## 분석 결과 요구사항

다음 JSON 형식으로 연말 정량 평가를 제공해주세요:

```json
{{
  "quarterlyPerformance": [
    {{
      "quarter": "1분기",
      "rating": "1st|2nd|3rd|4th|5th",
      "summary": "해당 분기의 주요 성과와 기여를 한국어로 요약 (50자 이내)"
    }},
    {{
      "quarter": "2분기", 
      "rating": "1st|2nd|3rd|4th|5th",
      "summary": "해당 분기의 주요 성과와 기여를 한국어로 요약 (50자 이내)"
    }},
    {{
      "quarter": "3분기",
      "rating": "1st|2nd|3rd|4th|5th", 
      "summary": "해당 분기의 주요 성과와 기여를 한국어로 요약 (50자 이내)"
    }},
    {{
      "quarter": "4분기",
      "rating": "1st|2nd|3rd|4th|5th",
      "summary": "해당 분기의 주요 성과와 기여를 한국어로 요약 (50자 이내)"
    }}
  ],
  "keyAchievements": [
    "연간 주요 성과 1 (구체적인 수치나 결과 포함, 한국어)",
    "연간 주요 성과 2 (구체적인 수치나 결과 포함, 한국어)", 
    "연간 주요 성과 3 (구체적인 수치나 결과 포함, 한국어)"
  ]
}}
```

## 평가 기준
1. **분기별 등급 (rating)**:
   - 1st: 목표 대비 120% 이상 달성, 탁월한 성과
   - 2nd: 목표 대비 100-119% 달성, 우수한 성과  
   - 3rd: 목표 대비 80-99% 달성, 양호한 성과
   - 4th: 목표 대비 60-79% 달성, 개선 필요
   - 5th: 목표 대비 60% 미만, 현저한 개선 필요

2. **주요 성과 (keyAchievements)**:
   - 정량적 결과가 포함된 구체적인 성과
   - 조직에 미친 긍정적 영향
   - 개인의 성장과 역량 발전

모든 내용은 **한국어**로 작성하고, 실제 데이터를 기반으로 객관적이고 구체적으로 평가해주세요.
JSON 형식을 정확히 준수하여 응답해주세요.
"""
        
        return prompt
    
    def execute_annual_evaluation(self, prompt):
        """연말 AI 평가 실행"""
        try:
            logger.info("연말 AI 평가 실행 시작")
            print("🤖 AI 연말 평가 분석 중...")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "당신은 전문적인 HR 연말 평가 에이전트입니다. 객관적이고 구체적인 성과 평가를 제공하며, 항상 정확한 JSON 형식으로 한국어로 응답합니다."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=3000
            )
            
            response_text = response.choices[0].message.content
            print("✅ AI 분석 완료")
            
            # JSON 추출 및 파싱
            json_text = self._extract_json(response_text)
            result = json.loads(json_text)
            
            logger.info("연말 AI 평가 완료")
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON 파싱 오류: {e}")
            print(f"❌ JSON 파싱 실패: {e}")
            return {
                "error": "JSON 파싱 실패",
                "raw_response": response_text if 'response_text' in locals() else "No response",
                "error_details": str(e)
            }
        except Exception as e:
            logger.error(f"AI 평가 오류: {e}")
            print(f"❌ AI 평가 실패: {e}")
            return {"error": str(e)}
    
    def _extract_json(self, response_text):
        """응답에서 JSON 부분 추출"""
        if "```json" in response_text:
            json_start = response_text.find("```json") + 7
            json_end = response_text.find("```", json_start)
            return response_text[json_start:json_end].strip()
        else:
            return response_text.strip()

# =====================================
# 메인 연말 평가 클래스
class AnnualEvaluationSystem:
    def __init__(self, config=None):
        self.config = config or Config()
        
        # 컴포넌트 초기화
        print("📊 MongoDB 연결 중...")
        self.mongo_manager = MongoDBManager(self.config.MONGODB_CONFIG)
        
        if not self.mongo_manager.connect():
            raise Exception("MongoDB 연결 실패")
        
        print("🤖 AI 평가기 초기화 중...")
        self.ai_evaluator = AnnualAIEvaluator(
            self.config.OPENAI_API_KEY,
            self.config.MODEL
        )
        
        # 출력 디렉토리 생성
        self.output_path = Path(self.config.OUTPUT_PATH)
        self.output_path.mkdir(exist_ok=True)
        
        logger.info("AnnualEvaluationSystem 초기화 완료")
    
    def evaluate_user_annual(self, user_id, year=2024):
        """단일 사용자 연말 평가"""
        logger.info(f"사용자 {user_id}의 {year}년 연말 평가 시작")
        
        try:
            # 1. 분기별 데이터 수집
            quarterly_data = self.mongo_manager.get_user_quarterly_data(user_id, year)
            
            if not quarterly_data:
                raise ValueError(f"사용자 {user_id}의 {year}년 데이터가 없습니다.")
            
            # 2. 데이터 분석
            analysis = QuarterlyDataAnalyzer.analyze_quarterly_data(quarterly_data)
            
            if not analysis:
                raise ValueError(f"사용자 {user_id}의 데이터 분석 실패")
            
            print(f"📊 분석 완료 - {analysis['total_quarters']}개 분기, {analysis['goals_summary']['total_unique_goals']}개 목표")
            
            # 3. AI 연말 평가
            prompt = self.ai_evaluator.generate_annual_prompt(quarterly_data, analysis)
            evaluation_result = self.ai_evaluator.execute_annual_evaluation(prompt)
            
            if "error" in evaluation_result:
                raise ValueError(f"AI 평가 실패: {evaluation_result['error']}")
            
            # 4. 최종 결과 생성
            final_result = self._create_final_annual_result(
                user_id, year, quarterly_data, analysis, evaluation_result
            )
            
            # 5. 결과 저장
            output_file = self._save_annual_result(final_result, user_id, year)
            
            logger.info(f"사용자 {user_id} 연말 평가 완료")
            return {
                "status": "success",
                "user_id": user_id,
                "year": year,
                "output_file": output_file,
                "quarters_analyzed": analysis['total_quarters'],
                "total_goals": analysis['goals_summary']['total_unique_goals']
            }
            
        except Exception as e:
            logger.error(f"사용자 {user_id} 연말 평가 실패: {e}")
            return {
                "status": "failed",
                "user_id": user_id,
                "year": year,
                "error": str(e)
            }
    
    def evaluate_batch_annual(self, user_ids=None, year=2024):
        """배치 연말 평가"""
        logger.info(f"{year}년 배치 연말 평가 시작")
        
        if user_ids is None:
            # 사용 가능한 사용자 조회
            available_users = self.mongo_manager.get_available_users(year)
            user_ids = [user["user_id"] for user in available_users]
        
        if not user_ids:
            return {"error": f"{year}년 데이터가 있는 사용자가 없습니다."}
        
        batch_results = {
            "batch_metadata": {
                "year": year,
                "start_time": datetime.now().isoformat(),
                "target_user_ids": user_ids,
                "total_users": len(user_ids)
            },
            "individual_results": {},
            "batch_summary": {
                "successful_users": 0,
                "failed_users": 0,
                "total_quarters_analyzed": 0,
                "total_goals_analyzed": 0
            }
        }
        
        for i, user_id in enumerate(user_ids, 1):
            print(f"\n📊 배치 연말 평가 진행: {i}/{len(user_ids)} - User {user_id}")
            
            result = self.evaluate_user_annual(user_id, year)
            batch_results["individual_results"][user_id] = result
            
            if result["status"] == "success":
                batch_results["batch_summary"]["successful_users"] += 1
                batch_results["batch_summary"]["total_quarters_analyzed"] += result.get("quarters_analyzed", 0)
                batch_results["batch_summary"]["total_goals_analyzed"] += result.get("total_goals", 0)
                print(f"✅ User {user_id} 연말 평가 성공")
            else:
                batch_results["batch_summary"]["failed_users"] += 1
                print(f"❌ User {user_id} 연말 평가 실패: {result['error']}")
        
        batch_results["batch_metadata"]["end_time"] = datetime.now().isoformat()
        
        # 배치 결과 저장
        self._save_batch_annual_results(batch_results, year)
        
        logger.info(f"{year}년 배치 연말 평가 완료 - 성공: {batch_results['batch_summary']['successful_users']}명")
        return batch_results
    
    def _create_final_annual_result(self, user_id, year, quarterly_data, analysis, ai_result):
        """최종 연말 결과 생성"""
        user_info = analysis["user_info"]
        
        final_result = {
            "type": "annual-evaluation",
            "year": year,
            "created_at": datetime.now().strftime("%Y-%m-%d"),
            "user": {
                "userId": int(user_id),
                "name": user_info.get("name", f"User_{user_id}"),
                "department": user_info.get("department", "미지정 부서")
            },
            
            # AI 생성 결과
            "quarterlyPerformance": ai_result.get("quarterlyPerformance", []),
            "keyAchievements": ai_result.get("keyAchievements", []),
            
            # 분석 메타데이터
            "analysisMetadata": {
                "quarters_analyzed": analysis["total_quarters"],
                "available_quarters": analysis["available_quarters"],
                "total_goals": analysis["goals_summary"]["total_unique_goals"],
                "consistent_goals": len(analysis["consistency_analysis"]["consistent_goals"]),
                "consistency_rate": round(analysis["consistency_analysis"]["consistency_rate"] * 100, 1)
            },
            
            # 원본 데이터 참조
            "rawDataSummary": {
                "quarterly_goal_counts": {
                    quarter: len(goals) for quarter, goals in analysis["goals_summary"]["quarterly_breakdown"].items()
                },
                "goal_frequency": analysis["consistency_analysis"]["goal_frequency"]
            }
        }
        
        return final_result
    
    def _save_annual_result(self, result, user_id, year):
        """연말 결과 저장"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"annual_evaluation_{user_id}_{year}_{timestamp}.json"
        
        output_file = self.output_path / filename
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        logger.info(f"연말 평가 결과 저장: {output_file}")
        return str(output_file)
    
    def _save_batch_annual_results(self, batch_results, year):
        """배치 연말 결과 저장"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"batch_annual_evaluation_{year}_{timestamp}.json"
        
        output_file = self.output_path / filename
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(batch_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"배치 연말 평가 결과 저장: {output_file}")
    
    def close(self):
        """시스템 종료"""
        self.mongo_manager.close()
        logger.info("AnnualEvaluationSystem 종료")

# =====================================
# 메인 실행 함수
def main():
    """메인 실행 함수"""
    
    print("🎯 === 연말 정량 평가 시스템 ===")
    print("📊 MongoDB 기반 AI 연말 종합 평가")
    print("🤖 분기별 데이터를 통합하여 연말 정량 결과 생성")
    
    system = None
    
    try:
        print("\n🤖 시스템 초기화 중...")
        
        # 설정 입력 받기
        config = Config()
        
        while True:
            print("\n🎯 === 메인 메뉴 ===")
            print("1. 단일 사용자 연말 평가")
            print("2. 전체 사용자 배치 연말 평가")
            print("3. 사용 가능한 사용자 목록 조회")
            print("4. 종료")
            
            choice = input("\n선택하세요 (1-4): ").strip()
            
            if choice == "1":
                user_id = input("평가할 사용자 ID를 입력하세요: ").strip()
                year = input("평가 연도를 입력하세요 (기본: 2024): ").strip()
                year = int(year) if year else 2024
                
                print(f"\n🚀 사용자 {user_id}의 {year}년 연말 평가 시작...")
                
                result = system.evaluate_user_annual(user_id, year)
                
                if result["status"] == "success":
                    print(f"\n🎉 === 연말 평가 완료 ===")
                    print(f"📊 분석된 분기: {result['quarters_analyzed']}개")
                    print(f"📋 분석된 목표: {result['total_goals']}개")
                    print(f"📁 결과 파일: {result['output_file']}")
                else:
                    print(f"❌ 연말 평가 실패: {result['error']}")
            
            elif choice == "2":
                year = input("평가 연도를 입력하세요 (기본: 2024): ").strip()
                year = int(year) if year else 2024
                
                # 사용 가능한 사용자 확인
                available_users = system.mongo_manager.get_available_users(year)
                
                if not available_users:
                    print(f"❌ {year}년 데이터가 있는 사용자가 없습니다.")
                    continue
                
                print(f"\n📊 {year}년 배치 연말 평가 대상:")
                for user in available_users:
                    print(f"  - User {user['user_id']}: {user['name']} ({user['department']}) - {user['available_quarters']}개 분기")
                
                confirm = input(f"\n총 {len(available_users)}명의 연말 평가를 진행하시겠습니까? (y/N): ").strip().lower()
                
                if confirm not in ['y', 'yes']:
                    print("❌ 배치 평가를 취소합니다.")
                    continue
                
                print(f"\n🚀 {year}년 배치 연말 평가 시작...")
                
                batch_result = system.evaluate_batch_annual(year=year)
                
                if "error" not in batch_result:
                    print(f"\n🎉 === 배치 연말 평가 완료 ===")
                    print(f"📊 대상 사용자: {batch_result['batch_metadata']['total_users']}명")
                    print(f"✅ 성공: {batch_result['batch_summary']['successful_users']}명")
                    print(f"❌ 실패: {batch_result['batch_summary']['failed_users']}명")
                    print(f"📈 총 분석된 분기: {batch_result['batch_summary']['total_quarters_analyzed']}개")
                    print(f"📋 총 분석된 목표: {batch_result['batch_summary']['total_goals_analyzed']}개")
                else:
                    print(f"❌ 배치 평가 실패: {batch_result['error']}")
            
            elif choice == "3":
                year = input("조회할 연도를 입력하세요 (기본: 2024): ").strip()
                year = int(year) if year else 2024
                
                users = system.mongo_manager.get_available_users(year)
                
                if users:
                    print(f"\n📋 {year}년 데이터가 있는 사용자 ({len(users)}명):")
                    for user in users:
                        print(f"  🔹 User {user['user_id']}: {user['name']} ({user['department']}) - {user['available_quarters']}개 분기")
                else:
                    print(f"❌ {year}년 데이터가 있는 사용자가 없습니다.")
            
            elif choice == "4":
                print("👋 시스템을 종료합니다.")
                break
            
            else:
                print("❌ 잘못된 선택입니다. 1-4 중에서 선택해주세요.")
        
    except KeyboardInterrupt:
        print("\n\n👋 사용자가 중단했습니다.")
    except Exception as e:
        logger.error(f"메인 실행 오류: {str(e)}")
        print(f"❌ 시스템 오류: {e}")
        
        # 에러 상세 정보 표시
        import traceback
        print(f"\n📋 에러 상세:")
        traceback.print_exc()
    
    finally:
        if system:
            system.close()


if __name__ == "__main__":
    main()