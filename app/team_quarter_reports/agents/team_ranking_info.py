import os
import pymongo
import pymysql
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict, List, Optional
import json

# 환경 변수 로드
load_dotenv()

class CompleteTeamAnalysisSystem:
    def __init__(self):
        self.maria_connection = None
        self.mongo_client = None
        self.mongo_db = None
        
    def connect_databases(self):
        """데이터베이스 연결"""
        try:
            # MariaDB 연결
            self.maria_connection = pymysql.connect(
                host=os.getenv('DB_HOST'),
                port=int(os.getenv('DB_PORT')),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=os.getenv('DB_NAME'),
                charset='utf8mb4'
            )
            print('✅ MariaDB 연결 성공')
            
            # MongoDB 연결
            mongo_url = f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/{os.getenv('MONGO_DB_NAME')}?authSource=admin"
            
            self.mongo_client = pymongo.MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
            self.mongo_client.admin.command('ping')
            self.mongo_db = self.mongo_client[os.getenv('MONGO_DB_NAME')]
            print('✅ MongoDB 연결 성공')
            
        except Exception as e:
            print(f'❌ 데이터베이스 연결 오류: {e}')
            raise e
    
    def get_user_job_mapping(self) -> Dict[int, Dict]:
        """사용자 ID별 조직 및 직군 정보 매핑"""
        try:
            with self.maria_connection.cursor(pymysql.cursors.DictCursor) as cursor:
                query = """
                    SELECT u.id, u.name, u.organization_id, u.job_id, j.name as job_name
                    FROM users u
                    LEFT JOIN jobs j ON u.job_id = j.id
                    WHERE u.organization_id IS NOT NULL
                    ORDER BY u.organization_id, u.id
                """
                cursor.execute(query)
                users = cursor.fetchall()
            
            user_mapping = {}
            for user in users:
                user_mapping[user['id']] = {
                    'name': user['name'],
                    'organization_id': user['organization_id'],
                    'job_id': user['job_id'],
                    'job_name': user['job_name']
                }
            
            print(f'✅ 사용자 매핑 정보 생성 완료: {len(user_mapping)}명')
            return user_mapping
            
        except Exception as e:
            print(f'❌ 사용자 매핑 생성 오류: {e}')
            return {}
    
    def get_organization_names(self) -> Dict[int, str]:
        """조직 ID별 이름 매핑"""
        try:
            with self.maria_connection.cursor(pymysql.cursors.DictCursor) as cursor:
                # organizations 테이블이 있는지 확인하고 조직 이름 가져오기
                try:
                    cursor.execute("SELECT division_id, name FROM organizations WHERE division_id IS NOT NULL")
                    orgs = cursor.fetchall()
                    org_mapping = {org['division_id']: org['name'] for org in orgs}
                    print(f'✅ 조직 이름 매핑 완료: {len(org_mapping)}개')
                    return org_mapping
                except:
                    # organizations 테이블이 없으면 기본 이름 사용
                    print('⚠️ organizations 테이블이 없습니다. 기본 조직명 사용')
                    return {
                        1: 'Cloud팀',
                        2: 'AI팀', 
                        3: 'Data팀',
                        4: 'ML팀'
                    }
        except Exception as e:
            print(f'❌ 조직 이름 매핑 오류: {e}')
            return {}
    
    def get_peer_keywords(self, user_id: int, year: int, quarter: int) -> List[str]:
        """특정 사용자의 동료평가 긍정 키워드 상위 3개 가져오기"""
        try:
            peer_collection = self.mongo_db['peer_evaluation_results']
            
            # 해당 분기 문서 조회
            peer_doc = peer_collection.find_one({
                'type': 'personal-quarter',
                'evaluated_year': year,
                'evaluated_quarter': quarter
            })
            
            if peer_doc and 'users' in peer_doc:
                # users 배열에서 해당 user_id 찾기
                for user in peer_doc['users']:
                    if user.get('user_id') == user_id:
                        keyword_summary = user.get('keyword_summary', {})
                        positive_keywords = keyword_summary.get('positive', [])
                        
                        if isinstance(positive_keywords, list) and len(positive_keywords) > 0:
                            # 상위 3개 키워드 추출
                            top_3 = []
                            for kw in positive_keywords[:3]:
                                if isinstance(kw, dict) and 'keyword' in kw:
                                    top_3.append(kw['keyword'])
                                elif isinstance(kw, str):
                                    top_3.append(kw)
                            return top_3 if top_3 else ['키워드없음']
                        else:
                            return ['키워드없음']
                
                return ['키워드없음']  # 해당 사용자를 찾지 못한 경우
            else:
                return ['키워드없음']  # 문서나 users 배열이 없는 경우
                
        except Exception as e:
            print(f'⚠️ 사용자 {user_id} 키워드 조회 오류: {e}')
            return ['키워드없음']
    
    def generate_team_member_analysis(self, org_id: int, org_name: str, year: int, quarter: int, user_mapping: Dict) -> Dict:
        """특정 팀의 멤버 분석 데이터 생성"""
        try:
            print(f"\n🔄 {org_name} (조직 {org_id})의 {year}년 {quarter}분기 멤버 분석 시작")
            
            # ranking_results에서 해당 분기 데이터 가져오기
            ranking_collection = self.mongo_db['ranking_results']
            ranking_doc = ranking_collection.find_one({
                'type': 'personal-quarter',
                'evaluated_year': year,
                'evaluated_quarter': quarter
            })
            
            if not ranking_doc:
                print(f"❌ {year}년 {quarter}분기 ranking 데이터를 찾을 수 없습니다.")
                return None
            
            # 해당 조직의 사용자들만 필터링
            org_users = [
                user for user in ranking_doc.get('users', [])
                if user.get('ranking_info', {}).get('organization_id') == org_id
            ]
            
            if not org_users:
                print(f"❌ 조직 {org_id}에 속한 사용자가 없습니다.")
                return None
            
            print(f"📊 조직 {org_id}: {len(org_users)}명 발견")
            
            # finalScore 기준으로 정렬 (내림차순)
            org_users.sort(key=lambda x: x.get('scores', {}).get('final_score', 0), reverse=True)
            
            # 각 사용자의 분석 데이터 생성
            member_analysis = []
            for rank, user in enumerate(org_users, 1):
                user_id = user['user_id']
                user_info = user_mapping.get(user_id, {})
                
                # 점수
                score = user.get('scores', {}).get('final_score', 0)
                
                # 동료평가 키워드 (사용 가능한 경우에만)
                peer_keywords = self.get_peer_keywords(user_id, year, quarter)
                
                # 직군 내 순위 계산
                job_rank = user.get('ranking_info', {}).get('same_job_rank', 0)
                job_total = user.get('ranking_info', {}).get('same_job_user_count', 0)
                
                if job_total > 0:
                    rank_percentage = (job_rank / job_total) * 100
                    overall_rank = f"상위 {rank_percentage:.1f}%"
                else:
                    overall_rank = "N/A"
                
                member_data = {
                    'rank': rank,
                    'name': user_info.get('name', f'사용자{user_id}'),
                    'score': round(score, 2),
                    'peerKeywords': peer_keywords,
                    'overallRank': overall_rank,
                    'role': user_info.get('job_name', 'Unknown')
                }
                
                member_analysis.append(member_data)
            
            # 상위 5명만 출력
            print(f"🏆 {org_name} 상위 5명:")
            for member in member_analysis[:5]:
                print(f"   {member['rank']}위: {member['name']} ({member['score']}점, {member['role']})")
            
            return {
                'type': 'team-quarter',
                'organization_id': org_id,
                'organization_name': org_name,
                'evaluated_year': year,
                'evaluated_quarter': quarter,
                'memberAnalysis': member_analysis,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
        except Exception as e:
            print(f'❌ 조직 {org_id} 멤버 분석 생성 오류: {e}')
            import traceback
            traceback.print_exc()
            return None
    
    def save_team_analysis(self, analysis_data: Dict) -> bool:
        """팀 분석 결과를 ranking_results 컬렉션에 저장"""
        try:
            if not analysis_data:
                return False
            
            collection = self.mongo_db['ranking_results']
            
            # 기존 데이터가 있으면 업데이트, 없으면 삽입
            filter_query = {
                'type': 'team-quarter',
                'organization_id': analysis_data['organization_id'],
                'evaluated_year': analysis_data['evaluated_year'],
                'evaluated_quarter': analysis_data['evaluated_quarter']
            }
            
            result = collection.replace_one(filter_query, analysis_data, upsert=True)
            
            if result.upserted_id:
                print(f'✅ 조직 {analysis_data["organization_id"]} 팀 분석 결과 신규 저장: {result.upserted_id}')
            else:
                print(f'✅ 조직 {analysis_data["organization_id"]} 팀 분석 결과 업데이트 완료')
            
            return True
            
        except Exception as e:
            print(f'❌ 팀 분석 결과 저장 오류: {e}')
            return False
    
    def get_available_quarters(self) -> List[tuple]:
        """사용 가능한 분기 데이터 목록 조회"""
        try:
            collection = self.mongo_db['ranking_results']
            pipeline = [
                {"$match": {"type": "personal-quarter"}},
                {"$group": {
                    "_id": {
                        "year": "$evaluated_year", 
                        "quarter": "$evaluated_quarter"
                    }
                }},
                {"$sort": {"_id.year": 1, "_id.quarter": 1}}
            ]
            
            quarters = list(collection.aggregate(pipeline))
            quarter_list = [(q['_id']['year'], q['_id']['quarter']) for q in quarters]
            
            print(f"📅 사용 가능한 분기: {quarter_list}")
            return quarter_list
            
        except Exception as e:
            print(f'❌ 분기 데이터 조회 오류: {e}')
            return []
    
    def process_all_teams_all_quarters(self):
        """모든 팀의 모든 분기 멤버 분석 처리"""
        try:
            print(f"\n🚀 모든 팀 모든 분기 멤버 분석 시작")
            
            # 1. 사용자 정보 매핑 가져오기
            user_mapping = self.get_user_job_mapping()
            
            # 2. 조직 이름 매핑 가져오기
            org_name_mapping = self.get_organization_names()
            
            # 3. 사용 가능한 분기 목록 가져오기
            available_quarters = self.get_available_quarters()
            
            if not available_quarters:
                print("❌ 처리할 분기 데이터가 없습니다.")
                return
            
            # 4. 조직 목록 가져오기
            with self.maria_connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute("""
                    SELECT organization_id, COUNT(*) as user_count
                    FROM users 
                    WHERE organization_id IS NOT NULL
                    GROUP BY organization_id
                    ORDER BY organization_id
                """)
                org_counts = cursor.fetchall()
            
            print(f"📋 처리 대상: {len(org_counts)}개 조직 × {len(available_quarters)}개 분기 = {len(org_counts) * len(available_quarters)}개 작업")
            
            total_success = 0
            total_fail = 0
            
            # 5. 각 분기별로 모든 조직 처리
            for year, quarter in available_quarters:
                print(f"\n{'='*60}")
                print(f"📅 {year}년 {quarter}분기 처리 시작")
                print(f"{'='*60}")
                
                quarter_success = 0
                quarter_fail = 0
                
                for org in org_counts:
                    org_id = org['organization_id']
                    org_name = org_name_mapping.get(org_id, f'조직{org_id}')
                    
                    # 팀 분석 데이터 생성
                    analysis_data = self.generate_team_member_analysis(
                        org_id, org_name, year, quarter, user_mapping
                    )
                    
                    if analysis_data:
                        # 저장
                        if self.save_team_analysis(analysis_data):
                            quarter_success += 1
                            total_success += 1
                        else:
                            quarter_fail += 1
                            total_fail += 1
                    else:
                        quarter_fail += 1
                        total_fail += 1
                
                print(f"📊 {year}년 {quarter}분기 결과: 성공 {quarter_success}개, 실패 {quarter_fail}개")
            
            print(f"\n🎉 전체 처리 완료!")
            print(f"✅ 총 성공: {total_success}개")
            print(f"❌ 총 실패: {total_fail}개")
            
        except Exception as e:
            print(f'❌ 전체 팀 분석 처리 오류: {e}')
            raise e
    
    def show_saved_results_summary(self):
        """저장된 결과 요약 확인"""
        try:
            print(f"\n📊 저장된 팀 분석 결과 요약")
            print("="*60)
            
            collection = self.mongo_db['ranking_results']
            
            # team-quarter 타입 문서 조회
            team_docs = list(collection.find({'type': 'team-quarter'}))
            
            if not team_docs:
                print("❌ 저장된 팀 분석 결과가 없습니다.")
                return
            
            print(f"📋 총 {len(team_docs)}개의 팀 분석 결과 저장됨")
            
            # 분기별 그룹화
            by_quarter = {}
            for doc in team_docs:
                key = f"{doc['evaluated_year']}년 {doc['evaluated_quarter']}분기"
                if key not in by_quarter:
                    by_quarter[key] = []
                by_quarter[key].append(doc)
            
            for quarter_key, docs in sorted(by_quarter.items()):
                print(f"\n🗓️ {quarter_key}:")
                for doc in sorted(docs, key=lambda x: x['organization_id']):
                    org_id = doc['organization_id']
                    org_name = doc['organization_name']
                    member_count = len(doc.get('memberAnalysis', []))
                    print(f"   조직 {org_id} ({org_name}): {member_count}명")
                    
                    # 1위 사용자 정보 출력
                    if doc.get('memberAnalysis'):
                        top_member = doc['memberAnalysis'][0]
                        print(f"     1위: {top_member['name']} ({top_member['score']}점, {top_member['role']})")
            
            # 샘플 결과 상세 출력 (조직 1, 가장 최근 분기)
            latest_doc = max(team_docs, key=lambda x: (x['evaluated_year'], x['evaluated_quarter']))
            if latest_doc['organization_id'] == 1:
                sample_doc = latest_doc
            else:
                sample_doc = next((doc for doc in team_docs if doc['organization_id'] == 1), latest_doc)
            
            if sample_doc:
                print(f"\n📝 샘플 결과 상세 ({sample_doc['organization_name']}, {sample_doc['evaluated_year']}년 {sample_doc['evaluated_quarter']}분기):")
                for member in sample_doc.get('memberAnalysis', [])[:3]:
                    print(f"   {member['rank']}. {member['name']}")
                    print(f"      점수: {member['score']}")
                    print(f"      직군: {member['role']}")
                    print(f"      키워드: {member['peerKeywords']}")
                    print(f"      직군 내 순위: {member['overallRank']}")
                    print()
                
        except Exception as e:
            print(f'❌ 결과 요약 확인 오류: {e}')
    
    def disconnect_databases(self):
        """데이터베이스 연결 해제"""
        try:
            if self.maria_connection:
                self.maria_connection.close()
                print('✅ MariaDB 연결 해제')
            if self.mongo_client:
                self.mongo_client.close()
                print('✅ MongoDB 연결 해제')
        except Exception as e:
            print(f'❌ 데이터베이스 연결 해제 오류: {e}')

def main():
    system = CompleteTeamAnalysisSystem()
    
    try:
        system.connect_databases()
        
        # 모든 팀의 모든 분기 분석 처리
        system.process_all_teams_all_quarters()
        
        # 저장된 결과 요약 확인
        system.show_saved_results_summary()
        
    except Exception as e:
        print(f'❌ 메인 처리 오류: {e}')
        import traceback
        traceback.print_exc()
    finally:
        system.disconnect_databases()

if __name__ == '__main__':
    main()