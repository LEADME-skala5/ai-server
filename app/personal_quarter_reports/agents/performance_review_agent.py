import os
import json
import pymysql
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv
from pathlib import Path
from pymongo import MongoClient
from openai import OpenAI
import logging

# 환경변수 로드
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# DB 설정
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": True
}

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB 설정
MONGO_CONFIG = {
    "host": os.getenv("MONGO_HOST"),
    "port": int(os.getenv("MONGO_PORT")),
    "username": os.getenv("MONGO_USER"),
    "password": os.getenv("MONGO_PASSWORD"),
    "db_name": os.getenv("MONGO_DB_NAME")
}

class MongoDBManager:
    """MongoDB 연결 및 관리 클래스"""
    
    def __init__(self):
        self.mongodb_uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
        self.database_name = MONGO_CONFIG["db_name"]
        self.output_collection_name = "final_performance_reviews"
        self.client = None
        print(f"📋 MongoDB 설정 로드 완료: {MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/{self.database_name}")
    
    def connect(self):
        """MongoDB 연결"""
        try:
            self.client = MongoClient(self.mongodb_uri)
            self.client.admin.command('ping')
            print("✅ MongoDB 연결 성공!")
            return True
        except Exception as e:
            print(f"❌ MongoDB 연결 실패: {e}")
            return False
    
    def get_user_data_from_collection(self, collection_name: str, user_id: int, year: int, quarter: int) -> Optional[Dict]:
        """특정 컬렉션에서 사용자 데이터 조회"""
        try:
            if not self.client:
                if not self.connect():
                    return None
            
            db = self.client[self.database_name]
            collection = db[collection_name]
            
            document = collection.find_one({
                "type": "personal-quarter",
                "evaluated_year": year,
                "evaluated_quarter": quarter
            })
            
            if not document or "users" not in document:
                return None
            
            for user_data in document["users"]:
                if user_data.get("user_id") == user_id:
                    return user_data
            
            return None
            
        except Exception as e:
            print(f"❌ MongoDB 데이터 조회 실패 (collection: {collection_name}, user: {user_id}): {e}")
            return None
    
    def add_user_to_quarter_document(self, user_data: Dict) -> bool:
        """분기별 문서에 사용자 데이터 추가/업데이트 - 자동 덮어쓰기"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            
            db = self.client[self.database_name]
            collection = db[self.output_collection_name]
            
            existing_doc = collection.find_one({
                "type": "personal-quarter",
                "evaluated_year": user_data['year'],
                "evaluated_quarter": user_data['quarter']
            })
            
            if existing_doc:
                existing_users = existing_doc.get("users", [])
                user_exists = False
                updated_users = []
                
                for existing_user in existing_users:
                    if existing_user.get("user_id") == user_data["user_id"]:
                        updated_users.append(user_data)
                        user_exists = True
                        print(f"🔄 사용자 ID {user_data['user_id']} 기존 데이터 덮어쓰기")
                    else:
                        updated_users.append(existing_user)
                
                if not user_exists:
                    updated_users.append(user_data)
                    print(f"✅ 기존 분기 문서에 사용자 ID {user_data['user_id']} 새로 추가")
                
                collection.update_one(
                    {
                        "type": "personal-quarter",
                        "evaluated_year": user_data['year'],
                        "evaluated_quarter": user_data['quarter']
                    },
                    {
                        "$set": {
                            "users": updated_users,
                            "user_count": len(updated_users),
                            "updated_at": datetime.now()
                        }
                    }
                )
                
            else:
                quarter_document = {
                    "type": "personal-quarter",
                    "evaluated_year": user_data['year'],
                    "evaluated_quarter": user_data['quarter'],
                    "user_count": 1,
                    "users": [user_data],
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }
                
                result = collection.insert_one(quarter_document)
                print(f"✅ 새로운 분기 문서 생성 및 사용자 ID {user_data['user_id']} 추가 완료")
            
            return True
            
        except Exception as e:
            print(f"❌ MongoDB 사용자 데이터 처리 실패 (사용자 ID: {user_data.get('user_id', 'unknown')}): {e}")
            return False
    
    def close(self):
        """MongoDB 연결 종료"""
        if self.client:
            self.client.close()
            print("MongoDB 연결 종료")

class PerformanceReviewAgent:
    """MongoDB 기반 성과 검토 및 최종 평가 에이전트"""
    
    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OpenAI API 키를 찾을 수 없습니다.")
        
        masked_key = api_key[:10] + "*" * (len(api_key) - 15) + api_key[-5:]
        print(f"🔑 OpenAI API 키 로드 완료: {masked_key}")
        self.client = OpenAI(api_key=api_key)
        self.mongodb_manager = MongoDBManager()
    
    def get_user_info(self, user_id: int) -> Dict:
        """사용자 기본 정보 조회"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT u.name, u.organization_id, j.name as job_name, u.job_years
                    FROM users u LEFT JOIN jobs j ON u.job_id = j.id
                    WHERE u.id = %s
                """, (user_id,))
                result = cur.fetchone()
                
                if result:
                    return {
                        "name": result['name'],
                        "job_name": result['job_name'] or "미지정",
                        "job_years": result['job_years'] or 0,
                        "organization_id": result['organization_id']
                    }
        except Exception as e:
            logger.warning(f"사용자 정보 조회 실패 (user_id: {user_id}): {e}")
        finally:
            if 'conn' in locals():
                conn.close()
        
        return {"name": f"직원 {user_id}번", "job_name": "미지정", "job_years": 0, "organization_id": None}
    
    def collect_all_evaluation_data(self, user_id: int, year: int, quarter: int) -> Dict:
        """모든 평가 데이터 수집"""
        print(f"🔍 사용자 ID {user_id}의 {year}Q{quarter} 평가 데이터 수집 중...")
        
        peer_data = self.mongodb_manager.get_user_data_from_collection("peer_evaluation_results", user_id, year, quarter)
        qualitative_data = self.mongodb_manager.get_user_data_from_collection("qualitative_evaluation_results", user_id, year, quarter)
        weekly_data = self.mongodb_manager.get_user_data_from_collection("weekly_evaluation_results", user_id, year, quarter)
        ranking_data = self.mongodb_manager.get_user_data_from_collection("ranking_results", user_id, year, quarter)
        
        data_status = {
            "peer_evaluation": "✅" if peer_data else "❌",
            "qualitative_evaluation": "✅" if qualitative_data else "❌",
            "weekly_evaluation": "✅" if weekly_data else "❌",
            "ranking": "✅" if ranking_data else "❌"
        }
        
        print(f"📊 데이터 수집 상태: {data_status}")
        
        return {
            "peer_evaluation": peer_data,
            "qualitative_evaluation": qualitative_data,
            "weekly_evaluation": weekly_data,
            "ranking": ranking_data,
            "collection_status": data_status
        }
    
    def safe_extract_keywords(self, keyword_list: List, field_name: str = 'keyword') -> List[str]:
        """키워드 리스트에서 안전하게 문자열 추출"""
        result = []
        for item in keyword_list:
            if isinstance(item, dict):
                keyword = (item.get(field_name) or item.get('name') or item.get('text') or 
                          item.get('title') or item.get('description') or str(item))
                result.append(str(keyword))
            else:
                result.append(str(item))
        return result
    
    def generate_activity_summary(self, all_data: Dict) -> tuple:
        """활동 요약 생성"""
        activities = []
        total_count = 0
        
        # 주간평가 데이터에서 활동 추출
        weekly_data = all_data.get("weekly_evaluation", {})
        if weekly_data:
            team_goals = weekly_data.get("teamGoals", [])
            key_achievements = weekly_data.get("keyAchievements", [])
            
            if team_goals:
                goal_count = len([g for g in team_goals if isinstance(g, dict) and g.get('assigned') == '배정'])
                if goal_count > 0:
                    activities.append(f"- [팀 목표 참여] {goal_count}건: 팀 목표 달성에 기여 ▶ 평가: 양호")
                    total_count += goal_count
            
            if key_achievements:
                achievement_count = len(key_achievements)
                if achievement_count > 0:
                    if isinstance(key_achievements[0], dict):
                        achievement_names = self.safe_extract_keywords(key_achievements[:2], 'achievement')
                        if not achievement_names:
                            achievement_names = self.safe_extract_keywords(key_achievements[:2], 'description')
                        activities.append(f"- [주요 성과] {achievement_count}건: {', '.join(achievement_names)} 등 ▶ 평가: 우수")
                    else:
                        activities.append(f"- [주요 성과] {achievement_count}건: {', '.join(map(str, key_achievements[:2]))} 등 ▶ 평가: 우수")
                    total_count += achievement_count
        
        # 동료평가에서 활동 유추
        peer_data = all_data.get("peer_evaluation", {})
        if peer_data:
            keyword_summary = peer_data.get("keyword_summary", {})
            positive_keywords = keyword_summary.get('positive', [])
            
            if positive_keywords:
                if isinstance(positive_keywords[0], dict):
                    keyword_names = self.safe_extract_keywords(positive_keywords[:3])
                    activities.append(f"- [협업 활동] 다수건: {', '.join(keyword_names)} 관련 업무 ▶ 평가: 우수")
                else:
                    activities.append(f"- [협업 활동] 다수건: {', '.join(map(str, positive_keywords[:3]))} 관련 업무 ▶ 평가: 우수")
                total_count += 3
        
        if not activities:
            activities.append("- [일반 업무] 정기 업무 수행 ▶ 평가: 보통")
            total_count = 1
        
        return '\n'.join(activities), total_count
    
    def extract_pattern_analysis(self, all_data: Dict) -> Dict:
        """정성 패턴 분석"""
        strengths = []
        weaknesses = []
        work_style = "일반적"
        
        # 동료평가에서 패턴 추출
        peer_data = all_data.get("peer_evaluation", {})
        if peer_data:
            keyword_summary = peer_data.get("keyword_summary", {})
            positive_keywords = keyword_summary.get('positive', [])
            negative_keywords = keyword_summary.get('negative', [])
            
            if positive_keywords:
                if len(positive_keywords) > 0 and isinstance(positive_keywords[0], dict):
                    keyword_names = self.safe_extract_keywords(positive_keywords[:3])
                    strengths.extend(keyword_names)
                else:
                    strengths.extend([str(k) for k in positive_keywords[:3]])
            
            if negative_keywords:
                if len(negative_keywords) > 0 and isinstance(negative_keywords[0], dict):
                    keyword_names = self.safe_extract_keywords(negative_keywords[:2])
                    weaknesses.extend(keyword_names)
                else:
                    weaknesses.extend([str(k) for k in negative_keywords[:2]])
        
        # 정성평가에서 패턴 추출
        qualitative_data = all_data.get("qualitative_evaluation", {})
        if qualitative_data:
            work_attitude = qualitative_data.get("work_attitude", [])
            if work_attitude:
                if len(work_attitude) > 0 and isinstance(work_attitude[0], dict):
                    attitude_names = self.safe_extract_keywords(work_attitude[:2], 'attitude')
                    if not attitude_names:
                        attitude_names = self.safe_extract_keywords(work_attitude[:2], 'description')
                    work_style = f"{', '.join(attitude_names)} 중심"
                else:
                    work_style = f"{', '.join(map(str, work_attitude[:2]))} 중심"
        
        if not strengths:
            strengths = ["성실성", "책임감"]
        if not weaknesses:
            weaknesses = ["소통", "효율성"]
        
        return {
            "strengths": ', '.join(strengths),
            "weaknesses": ', '.join(weaknesses),
            "work_style": work_style
        }
    
    def generate_new_format_prompt(self, user_id: int, year: int, quarter: int, all_data: Dict) -> str:
        """AI 프롬프트 생성"""
        user_info = self.get_user_info(user_id)
        name = user_info["name"]
        job_name = user_info["job_name"]
        job_years = user_info["job_years"]
        
        peer_data = all_data.get("peer_evaluation", {})
        qualitative_data = all_data.get("qualitative_evaluation", {})
        weekly_data = all_data.get("weekly_evaluation", {})
        ranking_data = all_data.get("ranking", {})
        
        activity_summary, total_activities = self.generate_activity_summary(all_data)
        pattern_analysis = self.extract_pattern_analysis(all_data)
        
        # 데이터 값 추출 및 기본값 설정
        peer_score = peer_data.get("peer_evaluation_score", 0) if peer_data else 0
        
        if peer_data and peer_data.get("keyword_summary"):
            keyword_summary = peer_data.get("keyword_summary", {})
            positive_keywords = keyword_summary.get('positive', [])
            negative_keywords = keyword_summary.get('negative', [])
            
            peer_keywords_pos = ', '.join(self.safe_extract_keywords(positive_keywords)) if positive_keywords else "협업"
            peer_keywords_neg = ', '.join(self.safe_extract_keywords(negative_keywords)) if negative_keywords else "소통"
        else:
            peer_keywords_pos = "협업"
            peer_keywords_neg = "소통"
        
        peer_feedback_summary = peer_data.get("feedback", "동료들과 원활한 협업을 보여줌") if peer_data else "동료들과 원활한 협업을 보여줌"
        qualitative_score = qualitative_data.get("qualitative_score", 0) if qualitative_data else 0
        
        if qualitative_data and qualitative_data.get("work_attitude"):
            work_attitude_list = qualitative_data.get("work_attitude", [])
            if len(work_attitude_list) > 0 and isinstance(work_attitude_list[0], dict):
                attitude_summary = ', '.join(self.safe_extract_keywords(work_attitude_list, 'attitude'))
                if not attitude_summary:
                    attitude_summary = ', '.join(self.safe_extract_keywords(work_attitude_list, 'description'))
            else:
                attitude_summary = ', '.join(map(str, work_attitude_list))
        else:
            attitude_summary = "성실함"
        
        if weekly_data:
            weekly_score = 0
            if weekly_data.get("evaluationScore"):
                eval_score = weekly_data.get("evaluationScore")
                if isinstance(eval_score, dict):
                    weekly_score = eval_score.get("weeklyScore", 0)
                else:
                    weekly_score = float(eval_score) if eval_score else 0
            
            team_goals = weekly_data.get("teamGoals", [])
            num_team_goals = 0
            if team_goals:
                num_team_goals = len([g for g in team_goals if isinstance(g, dict) and g.get('assigned') == '배정'])
            
            key_achievements = weekly_data.get("keyAchievements", [])
            if key_achievements:
                if len(key_achievements) > 0 and isinstance(key_achievements[0], dict):
                    achievement_names = self.safe_extract_keywords(key_achievements[:3], 'achievement')
                    if not achievement_names:
                        achievement_names = self.safe_extract_keywords(key_achievements[:3], 'description')
                    key_achievements_str = ', '.join(achievement_names) if achievement_names else "정기 업무 수행"
                else:
                    key_achievements_str = ', '.join(map(str, key_achievements[:3]))
            else:
                key_achievements_str = "정기 업무 수행"
        else:
            weekly_score = 0
            num_team_goals = 0
            key_achievements_str = "정기 업무 수행"
        
        if ranking_data:
            ranking_info = ranking_data.get("ranking_info", {})
            scores = ranking_data.get("scores", {})
            rank_in_jobgroup = ranking_info.get('rank', 'N/A')
            total_in_jobgroup = ranking_info.get('total_in_group', 'N/A')
            rank_in_team = ranking_info.get('team_rank', 'N/A')
            total_in_team = ranking_info.get('total_in_team', 'N/A')
            final_score = scores.get('final_score', 0)
        else:
            rank_in_jobgroup = total_in_jobgroup = rank_in_team = total_in_team = 'N/A'
            final_score = 0
        
        prompt = f"""
당신은 기업 인사팀의 성과 분석 전문가이며, 작성한 결과물은 인사 평가 문서에 직접 활용됩니다.  
다음은 직원 {name}({user_id})의 {year}년 {quarter}분기 활동 데이터입니다.  
이 정보를 바탕으로 아래 항목을 작성하십시오:

1. **분기 성과 종합 요약 (총평)**

---
[입력 데이터 요약]  
- 평가 기간: {year}년 {quarter}분기  
- 직무 및 연차: {job_name}, {job_years}년차  
- 총 활동 수: {total_activities}건  

[주요 업무 활동 및 평가 요약]  
{activity_summary}

[정성 패턴 분석]  
- 강점: {pattern_analysis['strengths']}  
- 개선점: {pattern_analysis['weaknesses']}  
- 업무 스타일: {pattern_analysis['work_style']}

[동료 피드백 요약]  
- 동료평가 점수: {peer_score}/5.0  
- 긍정 키워드: {peer_keywords_pos}  
- 보완 키워드: {peer_keywords_neg}  
- 피드백 요약: {peer_feedback_summary}

[정성 평가 요약]  
- 평가 점수: {qualitative_score}/5.0  
- 업무 태도 평가: {attitude_summary}

[주간 평가]  
- 점수: {weekly_score}/5.0  
- 참여 팀 목표 수: {num_team_goals}개  
- 주요 성과: {key_achievements_str}

[랭킹]  
- 직군 내 순위: {rank_in_jobgroup}/{total_in_jobgroup}  
- 팀 내 순위: {rank_in_team}/{total_in_team}  
- 최종 점수: {final_score}/5.0

---
[작성 지침]  

**분기 성과 종합 요약 (`performance_summary`)**  
총 **5문장**으로 구성하며, 다음 문장 구조를 그대로 따르십시오:

① {name}님은 이번 분기 동안 {job_name} 직무에서 총 {total_activities}건의 활동을 수행하며 {{핵심 업무 요약}}을 중심으로 성과를 도출하였습니다.  
② {{핵심성과 항목}}에서 상위 {{퍼센트}}% 수준을 기록하였으며, 이는 {name} {{핵심 해석}}  
③ 동료들은 {name}님의 {{열거된 역량 키워드}}를 높이 평가했습니다.  
④ 이번 분기의 {name}님은 {{핵심 역량/태도 요약}} 측면에서 뚜렷한 특징을 보였습니다.  
⑤ 개선이 필요한 업무 항목으로는 {{업무 개선 항목}} 등이 있으며, 동료 피드백에서는 {{보완 키워드}}이 보완 키워드로 제시되었습니다.

**출력 형식은 반드시 아래 JSON 형태를 따르십시오:**  

```json
{{
  "performance_summary": "5문장 고정 구조의 성과 요약"
}}
```"""
        
        return prompt
    
    def generate_ai_review(self, prompt: str) -> Dict:
        """AI 기반 성과 검토 생성"""
        try:
            print("🤖 AI 기반 종합 성과 검토 생성 중...")
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1500,
                temperature=0.3
            )
            
            result_text = response.choices[0].message.content.strip()
            
            try:
                if "```json" in result_text and "```" in result_text:
                    start = result_text.find("```json") + 7
                    end = result_text.rfind("```")
                    json_text = result_text[start:end].strip()
                elif "```" in result_text:
                    start = result_text.find("```") + 3
                    end = result_text.rfind("```")
                    json_text = result_text[start:end].strip()
                else:
                    json_text = result_text
                
                result = json.loads(json_text)
                
                if "performance_summary" in result:
                    print("✅ AI 성과 검토 생성 완료")
                    return result
                else:
                    raise json.JSONDecodeError("구조 불완전", json_text, 0)
                    
            except json.JSONDecodeError as e:
                print(f"⚠️ JSON 파싱 실패: {str(e)}")
                print("🔧 텍스트에서 수동으로 내용 추출 시도...")
                
                lines = result_text.split('\n')
                performance_summary = ""
                
                in_summary = False
                for line in lines:
                    if '"performance_summary"' in line or 'performance_summary' in line:
                        in_summary = True
                        if ':' in line:
                            summary_start = line.split(':', 1)[1].strip().strip('"').strip(',')
                            if summary_start:
                                performance_summary = summary_start
                    elif in_summary and line.strip().startswith('"') and not line.strip().endswith('}'):
                        performance_summary += line.strip().strip('"').strip(',') + " "
                    elif in_summary and ('}' in line):
                        break
                
                if not performance_summary:
                    performance_summary = "AI 생성 결과를 파싱할 수 없어 종합 검토를 제공할 수 없습니다."
                
                print(f"✅ 수동 파싱 완료 - 검토문: {len(performance_summary)}자")
                
                return {"performance_summary": performance_summary.strip()}
                
        except Exception as e:
            logger.error(f"AI 성과 검토 생성 실패: {str(e)}")
            return {"performance_summary": f"성과 검토 생성 중 오류가 발생했습니다: {str(e)}"}
    
    def process_user_performance_review(self, user_id: int, year: int, quarter: int, save_to_mongodb: bool = True) -> Dict:
        """개별 사용자 성과 검토 처리"""
        try:
            print(f"\n🎯 사용자 ID {user_id}의 {year}Q{quarter} 성과 검토 생성 시작")
            
            all_data = self.collect_all_evaluation_data(user_id, year, quarter)
            
            available_data_count = sum(1 for data in [
                all_data["peer_evaluation"], all_data["qualitative_evaluation"], 
                all_data["weekly_evaluation"], all_data["ranking"]
            ] if data is not None)
            
            if available_data_count == 0:
                return {
                    "success": False,
                    "message": "해당 사용자의 평가 데이터가 없습니다.",
                    "data": None
                }
            
            print(f"📊 {available_data_count}/4개의 평가 데이터 발견")
            
            prompt = self.generate_new_format_prompt(user_id, year, quarter, all_data)
            ai_result = self.generate_ai_review(prompt)
            
            result_data = {
                "user_id": user_id,
                "year": year,
                "quarter": quarter,
                "performance_summary": ai_result.get("performance_summary", ""),
                "data_sources": all_data["collection_status"],
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            if save_to_mongodb:
                mongodb_save_success = self.mongodb_manager.add_user_to_quarter_document(result_data)
                
                if mongodb_save_success:
                    print(f"✅ 사용자 ID {user_id} 성과 검토 final_performance_reviews 컬렉션에 추가 완료")
                else:
                    print(f"❌ 사용자 ID {user_id} 성과 검토 MongoDB 저장 실패")
            
            return {"success": True, "data": result_data}
            
        except Exception as e:
            logger.error(f"성과 검토 처리 중 오류 발생: {str(e)}")
            return {
                "success": False,
                "message": f"처리 중 오류가 발생했습니다: {str(e)}",
                "data": None
            }
    
    def process_batch_performance_review(self, user_ids: List[int], year: int, quarter: int) -> List[Dict]:
        """배치 성과 검토 처리 - 자동 덮어쓰기"""
        results = []
        total_users = len(user_ids)
        
        print(f"🔄 자동 덮어쓰기 모드: 기존 데이터가 있으면 갱신")
        
        for i, user_id in enumerate(user_ids, 1):
            if i % 10 == 0 or i == total_users:
                print(f"처리 진행률: {i}/{total_users} ({i/total_users*100:.1f}%)")
            
            result = self.process_user_performance_review(user_id, year, quarter, save_to_mongodb=True)
            results.append(result)
            
            if result["success"]:
                print(f"✓ User {user_id}: 성과 검토 생성 완료 → final_performance_reviews 컬렉션에 저장 완료")
                
                data = result["data"]
                print(f"\n📋 === 사용자 ID {user_id} 성과 검토 결과 ===")
                print(f"🎯 분기 성과 종합 요약:")
                
                summary_text = data["performance_summary"]
                sentences = []
                
                if '\n' in summary_text:
                    sentences = [s.strip() for s in summary_text.split('\n') if s.strip()]
                else:
                    temp_sentences = summary_text.split('.')
                    for sentence in temp_sentences:
                        sentence = sentence.strip()
                        if sentence and len(sentence) > 10:
                            sentences.append(sentence + '.')
                
                if len(sentences) != 5:
                    sentences = [summary_text]
                
                for i, sentence in enumerate(sentences, 1):
                    if len(sentence) > 80:
                        wrapped_lines = []
                        words = sentence.split(' ')
                        current_line = f"   {i}. "
                        
                        for word in words:
                            if len(current_line + word + ' ') > 80:
                                wrapped_lines.append(current_line.rstrip())
                                current_line = "      " + word + ' '
                            else:
                                current_line += word + ' '
                        
                        if current_line.strip():
                            wrapped_lines.append(current_line.rstrip())
                        
                        for line in wrapped_lines:
                            print(line)
                    else:
                        print(f"   {i}. {sentence}")
                
                print(f"\n📊 데이터 소스: {data['data_sources']}")
                print(f"⏰ 처리 시간: {data['processed_at']}")
                print("=" * 60)
                
            else:
                print(f"✗ User {user_id}: {result['message']}")
        
        return results

def process_single_quarter_performance_review(agent: PerformanceReviewAgent, user_ids: List[int], year: int, quarter: int):
    """단일 분기 성과 검토 처리 - 자동 덮어쓰기"""
    print(f"\n=== {year}년 {quarter}분기 성과 검토 처리 시작 ===")
    print(f"처리할 사용자 수: {len(user_ids)}명")
    print(f"중복 처리 방식: 덮어쓰기 (기존 데이터 자동 갱신)")
    print(f"MongoDB 저장 방식: final_performance_reviews 컬렉션에 type: 'personal-quarter'로 구분")
    print("=" * 50)
    
    results = agent.process_batch_performance_review(user_ids, year, quarter)
    
    successful_count = sum(1 for r in results if r["success"])
    failed_count = len(results) - successful_count
    
    print(f"\n=== {quarter}분기 성과 검토 처리 완료 ===")
    print(f"성공: {successful_count}명 → final_performance_reviews 컬렉션에 저장 완료")
    print(f"실패: {failed_count}명")
    
    if successful_count > 0:
        print(f"\n📈 {quarter}분기 성과 검토 요약:")
        print(f"   - 총 {successful_count}명의 종합 성과 검토 완료")
        print(f"   - 평균 데이터 수집률: {successful_count * 4}/{len(user_ids) * 4} 모듈")
        print(f"   - AI 기반 맞춤형 성과 요약 생성")
        print(f"   - 중복 처리: 자동 덮어쓰기 모드")
    
    if failed_count > 0:
        print(f"⚠️  데이터가 부족하거나 처리 실패한 사용자: {failed_count}명")
    
    return {
        "quarter": quarter,
        "successful_count": successful_count,
        "failed_count": failed_count
    }

def main():
    print("🚀 MongoDB 기반 성과 검토 시스템 시작 (성과 요약만)")
    print("=" * 60)
    
    agent = PerformanceReviewAgent()
    
    print("🔌 MongoDB 연결 테스트...")
    if not agent.mongodb_manager.connect():
        print("❌ MongoDB 연결 실패. 프로그램을 종료합니다.")
        return
    
    evaluation_year = 2024
    
    print(f"\n=== {evaluation_year}년 전체 분기 성과 검토 배치 처리 시작 ===")
    print(f"입력 데이터 소스: 4개 분리된 컬렉션")
    print(f"  - peer_evaluation_results")
    print(f"  - qualitative_evaluation_results") 
    print(f"  - weekly_evaluation_results")
    print(f"  - ranking_results")
    print(f"저장 위치: MongoDB - {MONGO_CONFIG['db_name']}.final_performance_reviews")
    print(f"저장 방식: type: 'personal-quarter'로 구분")
    print(f"중복 처리: 자동 덮어쓰기 (기존 데이터 갱신)")
    print(f"출력 형식:")
    print(f"  - performance_summary: 5문장으로 구성된 성과 요약")
    print("=" * 60)
    
    user_ids = list(range(1, 101))
    all_quarters_results = {}
    
    for quarter in [1, 2, 3, 4]:
        quarter_result = process_single_quarter_performance_review(agent, user_ids, evaluation_year, quarter)
        all_quarters_results[f"Q{quarter}"] = quarter_result
        
        backup_filename = f"performance_review_final_{evaluation_year}Q{quarter}_backup.json"
        with open(backup_filename, 'w', encoding='utf-8') as f:
            json.dump(quarter_result, f, ensure_ascii=False, indent=2)
        print(f"📄 백업 파일 저장 완료: {backup_filename}")
        
        print("\n" + "=" * 60)
    
    print(f"\n🎉 {evaluation_year}년 전체 분기 성과 검토 처리 완료!")
    print("=" * 60)
    
    total_processed = 0
    for quarter in [1, 2, 3, 4]:
        if f"Q{quarter}" in all_quarters_results:
            quarter_data = all_quarters_results[f"Q{quarter}"]
            successful = quarter_data["successful_count"]
            total_processed += successful
            print(f"Q{quarter}: 성공 {successful}명 → type: 'personal-quarter', evaluated_year: {evaluation_year}, evaluated_quarter: {quarter}")
        else:
            print(f"Q{quarter}: 데이터 없음")
    
    print(f"\n🎉 처리 완료 요약:")
    print(f"  - 총 처리된 사용자: {total_processed}명")
    print(f"  - 중복 처리 방식: 자동 덮어쓰기 (기존 데이터 갱신)")
    print(f"  - 입력: 4개 분리된 컬렉션에서 데이터 수집")
    print(f"  - 출력: final_performance_reviews 컬렉션")
    print(f"  - 출력 형식:")
    print(f"    • performance_summary: 5문장 성과 요약")
    print(f"    • data_sources: 데이터 수집 현황")
    print(f"  - 저장 방식: type: 'personal-quarter'로 구분")
    print(f"  - 데이터베이스: {MONGO_CONFIG['db_name']}")
    print(f"  - 컬렉션: final_performance_reviews")
    print(f"  - 문서 개수: {len(all_quarters_results)}개 (각 분기별)")
    print(f"  - 문서 구조: type/evaluated_year/evaluated_quarter/user_count/users[]")
    print(f"  - AI 모델: GPT-4o")
    
    print(f"\n📋 분기별 상세 결과:")
    for quarter in [1, 2, 3, 4]:
        if f"Q{quarter}" in all_quarters_results:
            quarter_data = all_quarters_results[f"Q{quarter}"]
            successful = quarter_data["successful_count"] 
            failed = quarter_data["failed_count"]
            success_rate = (successful / (successful + failed)) * 100 if (successful + failed) > 0 else 0
            print(f"  📊 Q{quarter}: {successful}명 성공, {failed}명 실패 (성공률: {success_rate:.1f}%)")
        else:
            print(f"  📊 Q{quarter}: 데이터 없음")
    
    agent.mongodb_manager.close()
    return all_quarters_results

if __name__ == "__main__":
    main()