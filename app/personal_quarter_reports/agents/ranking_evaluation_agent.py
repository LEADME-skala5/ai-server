import os
import pymysql
import json
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv
from pathlib import Path
from pymongo import MongoClient

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
        self.collection_name = "ranking_results"  # 변경된 컬렉션명
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
    
    def add_user_to_quarter_document(self, user_data: Dict) -> bool:
        """분기별 문서에 사용자 데이터 추가 - 새로운 형식"""
        try:
            if not self.client:
                if not self.connect():
                    return False
            
            db = self.client[self.database_name]
            collection = db[self.collection_name]
            
            # 해당 분기 문서가 존재하는지 확인
            existing_doc = collection.find_one({
                "type": "personal-quarter",
                "evaluated_year": user_data['year'],
                "evaluated_quarter": user_data['quarter']
            })
            
            if existing_doc:
                # 기존 문서에 사용자 데이터 추가
                collection.update_one(
                    {
                        "type": "personal-quarter",
                        "evaluated_year": user_data['year'],
                        "evaluated_quarter": user_data['quarter']
                    },
                    {
                        "$push": {"users": user_data},
                        "$set": {"updated_at": datetime.now()},
                        "$inc": {"user_count": 1}
                    }
                )
                print(f"✅ 기존 분기 문서에 사용자 ID {user_data['user_id']} 추가 완료")
            else:
                # 새로운 분기 문서 생성
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
                print(f"✅ 새로운 분기 문서 생성 및 사용자 ID {user_data['user_id']} 추가 완료 - Document ID: {result.inserted_id}")
            
            return True
            
        except Exception as e:
            print(f"❌ MongoDB 사용자 데이터 추가 실패 (사용자 ID: {user_data.get('user_id', 'unknown')}): {e}")
            return False
    
    def close(self):
        """MongoDB 연결 종료"""
        if self.client:
            self.client.close()
            print("MongoDB 연결 종료")

class RankingEvaluationSystem:
    """랭킹 기반 평가 시스템"""
    
    def __init__(self):
        self.mongodb_manager = MongoDBManager()
    
    def get_db_connection(self):
        """MariaDB 연결"""
        return pymysql.connect(**DB_CONFIG)
    
    def get_user_ranking_data(self, user_id: int, evaluation_year: int, evaluation_quarter: int) -> Optional[Dict]:
        """사용자의 랭킹 데이터 조회"""
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                query = """
                SELECT 
                    uqs.user_id,
                    uqs.final_score,
                    uqs.user_rank,
                    uqs.team_rank,
                    uqs.weekly_score,
                    uqs.qualitative_score,
                    uqs.peer_score,
                    u.job_id,
                    u.job_years,
                    u.organization_id,
                    j.name as job_name
                FROM user_quarter_scores uqs
                JOIN users u ON uqs.user_id = u.id
                JOIN jobs j ON u.job_id = j.id
                WHERE uqs.user_id = %s 
                AND uqs.evaluation_year = %s 
                AND uqs.evaluation_quarter = %s
                AND uqs.final_score IS NOT NULL
                AND uqs.user_rank IS NOT NULL
                AND uqs.team_rank IS NOT NULL
                """
                
                cur.execute(query, (user_id, evaluation_year, evaluation_quarter))
                result = cur.fetchone()
                
                if not result:
                    return None
                
                # 동일 직군+연차 그룹의 총 인원 조회
                cur.execute("""
                    SELECT COUNT(*) as total_in_group
                    FROM user_quarter_scores uqs
                    JOIN users u ON uqs.user_id = u.id
                    WHERE u.job_id = %s 
                    AND u.job_years = %s
                    AND uqs.evaluation_year = %s 
                    AND uqs.evaluation_quarter = %s
                    AND uqs.final_score IS NOT NULL
                """, (result['job_id'], result['job_years'], evaluation_year, evaluation_quarter))
                
                group_info = cur.fetchone()
                total_in_group = group_info['total_in_group'] if group_info else 0
                
                # 동일 팀 총 인원 조회
                cur.execute("""
                    SELECT COUNT(*) as total_in_team
                    FROM user_quarter_scores uqs
                    JOIN users u ON uqs.user_id = u.id
                    WHERE u.organization_id = %s 
                    AND uqs.evaluation_year = %s 
                    AND uqs.evaluation_quarter = %s
                    AND uqs.final_score IS NOT NULL
                """, (result['organization_id'], evaluation_year, evaluation_quarter))
                
                team_info = cur.fetchone()
                total_in_team = team_info['total_in_team'] if team_info else 0
                
                return {
                    "user_id": result['user_id'],
                    "final_score": float(result['final_score']) if result['final_score'] else 0.0,
                    "user_rank": result['user_rank'],
                    "team_rank": result['team_rank'],
                    "weekly_score": float(result['weekly_score']) if result['weekly_score'] else 0.0,
                    "qualitative_score": float(result['qualitative_score']) if result['qualitative_score'] else 0.0,
                    "peer_score": float(result['peer_score']) if result['peer_score'] else 0.0,
                    "job_id": result['job_id'],
                    "job_years": result['job_years'],
                    "organization_id": result['organization_id'],
                    "job_name": result['job_name'],
                    "total_in_group": total_in_group,
                    "total_in_team": total_in_team
                }
                
        except Exception as e:
            print(f"❌ 사용자 랭킹 데이터 조회 오류: {e}")
            return None
        finally:
            conn.close()

    def get_all_users_with_ranking(self, evaluation_year: int, evaluation_quarter: int) -> List[int]:
        """해당 분기에 랭킹 데이터가 있는 모든 사용자 ID 조회"""
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT user_id 
                    FROM user_quarter_scores 
                    WHERE evaluation_year = %s 
                    AND evaluation_quarter = %s
                    AND final_score IS NOT NULL
                    AND user_rank IS NOT NULL
                    AND team_rank IS NOT NULL
                    ORDER BY user_id
                """, (evaluation_year, evaluation_quarter))
                
                results = cur.fetchall()
                return [row['user_id'] for row in results]
        except Exception as e:
            print(f"❌ 사용자 목록 조회 오류: {e}")
            return []
        finally:
            conn.close()
    
    def generate_ranking_evaluation_text(self, ranking_data: Dict) -> str:
        """랭킹 기반 간단한 결과문 생성"""
        user_rank = ranking_data['user_rank']
        team_rank = ranking_data['team_rank']
        total_in_group = ranking_data['total_in_group']
        total_in_team = ranking_data['total_in_team']
        job_name = ranking_data['job_name']
        job_years = ranking_data['job_years']
        final_score = ranking_data['final_score']
        
        # 직군+연차 순위와 팀 내 순위 모두 포함한 결과문 생성
        result_text = f"{job_name} {job_years}년차 그룹 내 {total_in_group}명 중 {user_rank}등, 팀 내 {total_in_team}명 중 {team_rank}등 (점수: {final_score:.2f})"
        
        return result_text
    
    def process_user_ranking_evaluation(self, user_id: int, evaluation_year: int, evaluation_quarter: int, save_to_mongodb: bool = True) -> Optional[Dict]:
        """개별 사용자 랭킹 평가 처리"""
        # 1. 랭킹 데이터 조회
        ranking_data = self.get_user_ranking_data(user_id, evaluation_year, evaluation_quarter)
        
        if not ranking_data:
            return {
                "success": False,
                "message": "해당 사용자의 랭킹 데이터가 없습니다.",
                "data": None
            }
        
        # 2. 평가문 생성
        result_text = self.generate_ranking_evaluation_text(ranking_data)
        
        # 3. 결과 구성
        result_data = {
            "user_id": ranking_data['user_id'],
            "year": evaluation_year,
            "quarter": evaluation_quarter,
            "ranking_info": {
                "job_name": ranking_data['job_name'],
                "job_years": ranking_data['job_years'],
                "same_job_rank": ranking_data['user_rank'],
                "same_job_user_count": ranking_data['total_in_group'],
                "organization_rank": ranking_data['team_rank'],
                "organization_user_count": ranking_data['total_in_team'],
                "organization_id": ranking_data['organization_id']
            },
            "scores": {
                "final_score": ranking_data['final_score'],
                "weekly_score": ranking_data['weekly_score'],
                "qualitative_score": ranking_data['qualitative_score'],
                "peer_score": ranking_data['peer_score']
            },
            "result_text": result_text,
            "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # 4. 분기별 문서에 사용자 데이터 추가
        if save_to_mongodb:
            mongodb_save_success = self.mongodb_manager.add_user_to_quarter_document(result_data)
            
            if mongodb_save_success:
                print(f"✅ 사용자 ID {user_id} 랭킹 평가 ranking_results 컬렉션에 추가 완료")
            else:
                print(f"❌ 사용자 ID {user_id} 랭킹 평가 MongoDB 저장 실패")
        
        result = {
            "success": True,
            "data": result_data
        }
        
        return result
    
    def process_batch_ranking_evaluation(self, user_ids: List[int], evaluation_year: int, evaluation_quarter: int) -> List[Dict]:
        """배치 랭킹 평가 처리 - ranking_results 컬렉션에 저장"""
        results = []
        total_users = len(user_ids)
        
        for i, user_id in enumerate(user_ids, 1):
            if i % 10 == 0 or i == total_users:
                print(f"처리 진행률: {i}/{total_users} ({i/total_users*100:.1f}%)")
            
            result = self.process_user_ranking_evaluation(user_id, evaluation_year, evaluation_quarter, save_to_mongodb=True)
            results.append(result)
            
            # 성공/실패 여부 출력
            if result["success"]:
                rank_info = result["data"]["ranking_info"]
                print(f"✓ User {user_id}: {rank_info['job_name']} {rank_info['job_years']}년차 {rank_info['same_job_rank']}/{rank_info['same_job_user_count']}등, 팀내 {rank_info['organization_rank']}/{rank_info['organization_user_count']}등 → ranking_results 컬렉션에 저장 완료")
            else:
                print(f"✗ User {user_id}: 랭킹 데이터 없음")
        
        return results
    
    def calculate_rankings_internal(self, evaluation_year: int, evaluation_quarter: int):
        """내부 랭킹 계산 함수 - 직군+연차 랭킹과 팀 내 랭킹 모두 계산"""
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                print(f"🎯 {evaluation_year}년 {evaluation_quarter}분기 랭킹 계산 시작")
                
                # 1. 특정 년도/분기의 user_quarter_scores 데이터와 user 정보 조인
                cur.execute("""
                    SELECT 
                        uqs.id AS final_id, 
                        uqs.user_id, 
                        uqs.final_score, 
                        u.job_id, 
                        u.job_years,
                        u.organization_id
                    FROM user_quarter_scores uqs
                    JOIN users u ON uqs.user_id = u.id
                    WHERE uqs.evaluation_year = %s 
                    AND uqs.evaluation_quarter = %s
                    AND uqs.final_score IS NOT NULL
                    ORDER BY u.job_id, u.job_years, uqs.final_score DESC
                """, (evaluation_year, evaluation_quarter))
                
                results = cur.fetchall()
                
                if not results:
                    print(f"❌ {evaluation_year}년 {evaluation_quarter}분기 final_score 데이터가 없습니다.")
                    return False
                
                print(f"📊 처리 대상: {len(results)}명 (final_score 보유자)")

                # 2. 직군+연차별 그룹핑 및 user_rank 계산
                from collections import defaultdict
                job_groups = defaultdict(list)
                for row in results:
                    key = (row['job_id'], row['job_years'])
                    job_groups[key].append(row)

                print(f"👥 직무+연차 그룹 수: {len(job_groups)}개")

                # 3. 각 직군+연차 그룹 내에서 user_rank 부여
                total_job_ranked = 0
                for key, group in job_groups.items():
                    job_id, job_years = key
                    
                    # final_score 기준 내림차순 정렬
                    sorted_group = sorted(
                        group, 
                        key=lambda x: float(x['final_score']) if x['final_score'] is not None else 0.0, 
                        reverse=True
                    )
                    
                    print(f"📋 직무ID {job_id}, {job_years}년차: {len(sorted_group)}명")
                    
                    # user_rank 부여 및 DB 업데이트
                    for idx, row in enumerate(sorted_group):
                        rank = idx + 1
                        cur.execute("""
                            UPDATE user_quarter_scores
                            SET user_rank = %s, updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, (rank, row['final_id']))
                        
                        total_job_ranked += 1
                        
                        # 상위 3명만 출력
                        if rank <= 3:
                            score = float(row['final_score']) if row['final_score'] else 0.0
                            print(f"   직군 {rank}위: user_id={row['user_id']}, score={score:.2f}")

                # 4. 팀별 그룹핑 및 team_rank 계산
                team_groups = defaultdict(list)
                for row in results:
                    key = row['organization_id']
                    team_groups[key].append(row)

                print(f"🏢 팀 그룹 수: {len(team_groups)}개")

                # 5. 각 팀 내에서 team_rank 부여
                total_team_ranked = 0
                for org_id, group in team_groups.items():
                    # final_score 기준 내림차순 정렬
                    sorted_group = sorted(
                        group, 
                        key=lambda x: float(x['final_score']) if x['final_score'] is not None else 0.0, 
                        reverse=True
                    )
                    
                    print(f"🏢 조직ID {org_id}: {len(sorted_group)}명")
                    
                    # team_rank 부여 및 DB 업데이트
                    for idx, row in enumerate(sorted_group):
                        team_rank = idx + 1
                        cur.execute("""
                            UPDATE user_quarter_scores
                            SET team_rank = %s, updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, (team_rank, row['final_id']))
                        
                        total_team_ranked += 1
                        
                        # 상위 3명만 출력
                        if team_rank <= 3:
                            score = float(row['final_score']) if row['final_score'] else 0.0
                            print(f"   팀내 {team_rank}위: user_id={row['user_id']}, score={score:.2f}")
                
                print(f"✅ 직군+연차 랭킹 계산 완료! 총 {total_job_ranked}명 user_rank 업데이트")
                print(f"✅ 팀 내 랭킹 계산 완료! 총 {total_team_ranked}명 team_rank 업데이트")
                return True
                
        except Exception as e:
            print(f"❌ 랭킹 계산 오류: {e}")
            return False
        finally:
            conn.close()

def process_single_quarter_ranking(system: RankingEvaluationSystem, user_ids: List[int], year: int, quarter: int):
    """단일 분기 랭킹 평가 처리 - ranking_results 컬렉션에 저장"""
    print(f"\n=== {year}년 {quarter}분기 랭킹 평가 처리 시작 ===")
    print(f"처리할 사용자 수: {len(user_ids)}명")
    print(f"MongoDB 저장 방식: ranking_results 컬렉션에 type: 'personal-quarter'로 구분")
    print("=" * 50)
    
    # 배치 처리 실행
    results = system.process_batch_ranking_evaluation(user_ids, year, quarter)
    
    # 결과 통계
    successful_count = sum(1 for r in results if r["success"])
    failed_count = len(results) - successful_count
    
    print(f"\n=== {quarter}분기 랭킹 평가 처리 완료 ===")
    print(f"성공: {successful_count}명 → ranking_results 컬렉션에 저장 완료")
    print(f"실패: {failed_count}명")
    
    # 점수 분포 통계
    avg_score = None
    if successful_count > 0:
        scores = [r["data"]["scores"]["final_score"] for r in results if r["success"]]
        if scores:
            avg_score = sum(scores) / len(scores)
            max_score = max(scores)
            min_score = min(scores)
            
            print(f"평균 점수: {avg_score:.2f}")
            print(f"최고 점수: {max_score:.2f}")
            print(f"최저 점수: {min_score:.2f}")
    
    # 실패한 사용자 개수만 출력
    if failed_count > 0:
        print(f"랭킹 데이터가 없는 사용자: {failed_count}명")
    
    return {
        "quarter": quarter,
        "successful_count": successful_count,
        "failed_count": failed_count,
        "average_score": round(avg_score, 2) if avg_score else 0
    }

def main():
    print("✅ .env 파일에서 설정 로드 완료")
    
    # 시스템 초기화
    system = RankingEvaluationSystem()
    
    # MongoDB 연결 테스트
    print("🔌 MongoDB 연결 테스트...")
    if not system.mongodb_manager.connect():
        print("❌ MongoDB 연결 실패. 프로그램을 종료합니다.")
        return
    
    # 평가 년도 설정
    evaluation_year = 2024
    
    print(f"\n🚀 {evaluation_year}년 전체 분기 랭킹 평가 처리 시작")
    print(f"저장 방식: ranking_results 컬렉션에 type: 'personal-quarter'로 구분")
    print(f"저장 위치: MongoDB - {MONGO_CONFIG['db_name']}.ranking_results")
    print(f"문서 구조:")
    print(f"  - type: 'personal-quarter'")
    print(f"  - evaluated_year: {evaluation_year}")
    print(f"  - evaluated_quarter: 1, 2, 3, 4")
    print(f"  - users: [사용자별 랭킹 데이터 배열]")
    print("=" * 60)
    
    # 전체 결과 저장용
    all_quarters_results = {}
    
    # 4개 분기 모두 처리
    for quarter in [1, 2, 3, 4]:
        print(f"\n🏆 Step {quarter}: {evaluation_year}년 {quarter}분기 랭킹 계산")
        print("=" * 50)
        
        # 1. 랭킹 계산 (user_rank와 team_rank 모두)
        ranking_success = system.calculate_rankings_internal(evaluation_year, quarter)
        if not ranking_success:
            print(f"❌ {quarter}분기 랭킹 계산 실패. 다음 분기로 넘어갑니다.")
            continue
        
        print(f"\n📊 Step {quarter}: {quarter}분기 랭킹 결과문 생성 및 MongoDB 저장")
        print("=" * 50)
        
        # 2. 랭킹 데이터가 있는 사용자 조회
        user_ids = system.get_all_users_with_ranking(evaluation_year, quarter)
        
        if not user_ids:
            print(f"❌ {evaluation_year}년 {quarter}분기에 랭킹 데이터가 있는 사용자가 없습니다.")
            continue
        
        print(f"처리할 사용자 수: {len(user_ids)}명")
        
        # 3. 랭킹 결과문 생성 및 MongoDB 저장
        quarter_result = process_single_quarter_ranking(system, user_ids, evaluation_year, quarter)
        all_quarters_results[f"Q{quarter}"] = quarter_result
        
        # 백업 파일도 저장
        backup_filename = f"ranking_evaluation_results_{evaluation_year}Q{quarter}_backup.json"
        with open(backup_filename, 'w', encoding='utf-8') as f:
            json.dump(quarter_result, f, ensure_ascii=False, indent=2)
        print(f"📄 백업 파일 저장 완료: {backup_filename}")
        
        # 분기 간 구분
        print("\n" + "=" * 60)
    
    # 전체 분기 통합 결과 출력
    print(f"\n🎉 {evaluation_year}년 전체 분기 랭킹 평가 처리 완료!")
    print("=" * 60)
    
    total_processed = 0
    for quarter in [1, 2, 3, 4]:
        if f"Q{quarter}" in all_quarters_results:
            quarter_data = all_quarters_results[f"Q{quarter}"]
            successful = quarter_data["successful_count"]
            total_processed += successful
            print(f"Q{quarter}: 성공 {successful}명 → type: 'personal-quarter', evaluated_year: {evaluation_year}, evaluated_quarter: {quarter}")
        else:
            print(f"Q{quarter}: 데이터 없음 또는 처리 실패")
    
    print(f"\n🎉 처리 완료 요약:")
    print(f"  - 총 처리된 사용자: {total_processed}명")
    print(f"  - 저장 방식: ranking_results 컬렉션에 type별로 구분")
    print(f"  - 데이터베이스: {MONGO_CONFIG['db_name']}")
    print(f"  - 컬렉션: ranking_results")
    print(f"  - 문서 개수: {len(all_quarters_results)}개 (각 분기별)")
    print(f"  - 문서 구조: type/evaluated_year/evaluated_quarter/user_count/users[]")
    print(f"  - MariaDB user_quarter_scores.user_rank, team_rank 업데이트 완료")
    
    # MongoDB 연결 종료
    system.mongodb_manager.close()
    
    return all_quarters_results

if __name__ == "__main__":
    main()