import os
import pymongo
import pymysql
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict, List, Optional
import json

# 환경 변수 로드
load_dotenv()

class AnnualTeamRankingSystem:
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
    
    def get_user_name_mapping(self) -> Dict[int, str]:
        """사용자 ID별 이름 매핑"""
        try:
            with self.maria_connection.cursor(pymysql.cursors.DictCursor) as cursor:
                query = "SELECT id, name FROM users WHERE name IS NOT NULL"
                cursor.execute(query)
                users = cursor.fetchall()
            
            user_mapping = {user['id']: user['name'] for user in users}
            print(f'✅ 사용자 이름 매핑 완료: {len(user_mapping)}명')
            return user_mapping
            
        except Exception as e:
            print(f'❌ 사용자 이름 매핑 오류: {e}')
            return {}
    
    def get_user_organization_mapping(self) -> Dict[int, Dict]:
        """사용자 ID별 조직 정보 매핑"""
        try:
            with self.maria_connection.cursor(pymysql.cursors.DictCursor) as cursor:
                query = """
                    SELECT u.id, u.name, u.organization_id, o.name as org_name
                    FROM users u
                    LEFT JOIN organizations o ON u.organization_id = o.division_id
                    WHERE u.organization_id IS NOT NULL
                    ORDER BY u.organization_id, u.id
                """
                cursor.execute(query)
                users = cursor.fetchall()
            
            user_org_mapping = {}
            for user in users:
                user_org_mapping[user['id']] = {
                    'name': user['name'],
                    'organization_id': user['organization_id'],
                    'organization_name': user['org_name'] or f"조직{user['organization_id']}"
                }
            
            print(f'✅ 사용자 조직 매핑 완료: {len(user_org_mapping)}명')
            return user_org_mapping
            
        except Exception as e:
            print(f'❌ 사용자 조직 매핑 오류: {e}')
            return {}
    
    def get_annual_final_scores(self, year: int) -> Dict[int, Dict]:
        """연말 최종 점수 데이터 가져오기"""
        try:
            collection = self.mongo_db['final_score_results']
            
            # personal-final-score-annual 타입 문서 조회
            final_score_doc = collection.find_one({
                'type': 'personal-final-score-annual',
                'evaluated_year': year
            })
            
            if not final_score_doc:
                print(f"❌ {year}년 연말 최종 점수 데이터를 찾을 수 없습니다.")
                return {}
            
            users_scores = {}
            users_data = final_score_doc.get('users', {})
            
            for user_id_str, user_data in users_data.items():
                try:
                    user_id = int(user_id_str)
                    final_score_info = user_data.get('final_score_info', {})
                    category_averages = final_score_info.get('category_averages', {})
                    
                    # 각 점수 추출 (없으면 0점)
                    overall_score = final_score_info.get('overall_final_score', 0)
                    quantitative = category_averages.get('quantitative', 0)
                    qualitative = category_averages.get('qualitative', 0)
                    peer = category_averages.get('peer', 0)
                    
                    users_scores[user_id] = {
                        'overall_final_score': round(overall_score, 2),
                        'quantitative': round(quantitative, 2),
                        'qualitative': round(qualitative, 2),
                        'peer': round(peer, 2)
                    }
                    
                except (ValueError, TypeError) as e:
                    print(f"⚠️ 사용자 {user_id_str} 점수 처리 오류: {e}")
                    continue
            
            print(f'✅ {year}년 연말 점수 데이터 로드 완료: {len(users_scores)}명')
            return users_scores
            
        except Exception as e:
            print(f'❌ 연말 점수 데이터 조회 오류: {e}')
            return {}
    
    def generate_team_annual_ranking(self, org_id: int, org_name: str, year: int, 
                                   user_org_mapping: Dict, annual_scores: Dict) -> Dict:
        """특정 팀의 연말 랭킹 데이터 생성"""
        try:
            print(f"\n🔄 {org_name} (조직 {org_id})의 {year}년 연말 랭킹 생성 시작")
            
            # 해당 조직의 사용자들 필터링
            org_users = []
            for user_id, user_info in user_org_mapping.items():
                if user_info['organization_id'] == org_id and user_id in annual_scores:
                    org_users.append({
                        'user_id': user_id,
                        'name': user_info['name'],
                        'scores': annual_scores[user_id]
                    })
            
            if not org_users:
                print(f"❌ 조직 {org_id}에 속한 사용자가 없거나 점수 데이터가 없습니다.")
                return None
            
            # overall_final_score 기준으로 내림차순 정렬
            org_users.sort(key=lambda x: x['scores']['overall_final_score'], reverse=True)
            
            print(f"📊 조직 {org_id}: {len(org_users)}명 랭킹 생성")
            
            # 각 사용자의 랭킹 데이터 생성
            team_ranking = []
            for rank, user in enumerate(org_users, 1):
                member_data = {
                    'rank': rank,
                    'name': user['name'],
                    'overall_final_score': user['scores']['overall_final_score'],
                    'quantitative': user['scores']['quantitative'],
                    'qualitative': user['scores']['qualitative'],
                    'peer': user['scores']['peer']
                }
                team_ranking.append(member_data)
            
            # 상위 5명 출력
            print(f"🏆 {org_name} 상위 5명:")
            for member in team_ranking[:5]:
                print(f"   {member['rank']}위: {member['name']} ({member['overall_final_score']}점)")
            
            return {
                'type': 'team-annual',
                'organization_id': org_id,
                'organization_name': org_name,
                'evaluated_year': year,
                'total_members': len(team_ranking),
                'team_ranking': team_ranking,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
        except Exception as e:
            print(f'❌ 조직 {org_id} 연말 랭킹 생성 오류: {e}')
            import traceback
            traceback.print_exc()
            return None
    
    def generate_quarterly_rankings_for_annual(self, org_id: int, org_name: str, 
                                             year: int, user_org_mapping: Dict, 
                                             annual_scores: Dict) -> List[Dict]:
        """연말 보고서용 분기별 랭킹 데이터 생성 (4개 분기)"""
        try:
            print(f"\n🔄 {org_name} (조직 {org_id})의 {year}년 분기별 랭킹 생성 시작")
            
            # 해당 조직의 사용자들 필터링
            org_users = []
            for user_id, user_info in user_org_mapping.items():
                if user_info['organization_id'] == org_id and user_id in annual_scores:
                    org_users.append({
                        'user_id': user_id,
                        'name': user_info['name'],
                        'scores': annual_scores[user_id]
                    })
            
            if not org_users:
                print(f"❌ 조직 {org_id}에 속한 사용자가 없거나 점수 데이터가 없습니다.")
                return []
            
            # overall_final_score 기준으로 내림차순 정렬
            org_users.sort(key=lambda x: x['scores']['overall_final_score'], reverse=True)
            
            quarterly_rankings = []
            
            # 1~4분기 각각에 대해 동일한 랭킹 생성 (연말 데이터 기준)
            for quarter in range(1, 5):
                team_ranking = []
                for rank, user in enumerate(org_users, 1):
                    member_data = {
                        'rank': rank,
                        'name': user['name'],
                        'overall_final_score': user['scores']['overall_final_score'],
                        'quantitative': user['scores']['quantitative'],
                        'qualitative': user['scores']['qualitative'],
                        'peer': user['scores']['peer']
                    }
                    team_ranking.append(member_data)
                
                quarterly_ranking = {
                    'type': 'team-quarter',
                    'organization_id': org_id,
                    'organization_name': org_name,
                    'evaluated_year': year,
                    'evaluated_quarter': quarter,
                    'total_members': len(team_ranking),
                    'team_ranking': team_ranking,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                
                quarterly_rankings.append(quarterly_ranking)
                print(f"📊 {org_name} {quarter}분기 랭킹 생성 완료: {len(team_ranking)}명")
            
            return quarterly_rankings
            
        except Exception as e:
            print(f'❌ 조직 {org_id} 분기별 랭킹 생성 오류: {e}')
            return []
    
    def save_ranking_data(self, ranking_data: Dict) -> bool:
        """랭킹 데이터를 ranking_results 컬렉션에 저장"""
        try:
            if not ranking_data:
                return False
            
            collection = self.mongo_db['ranking_results']
            
            # 기존 데이터가 있으면 업데이트, 없으면 삽입
            if ranking_data['type'] == 'team-annual':
                filter_query = {
                    'type': 'team-annual',
                    'organization_id': ranking_data['organization_id'],
                    'evaluated_year': ranking_data['evaluated_year']
                }
            else:  # team-quarter
                filter_query = {
                    'type': 'team-quarter',
                    'organization_id': ranking_data['organization_id'],
                    'evaluated_year': ranking_data['evaluated_year'],
                    'evaluated_quarter': ranking_data['evaluated_quarter']
                }
            
            result = collection.replace_one(filter_query, ranking_data, upsert=True)
            
            if result.upserted_id:
                print(f'✅ 조직 {ranking_data["organization_id"]} 랭킹 데이터 신규 저장: {result.upserted_id}')
            else:
                print(f'✅ 조직 {ranking_data["organization_id"]} 랭킹 데이터 업데이트 완료')
            
            return True
            
        except Exception as e:
            print(f'❌ 랭킹 데이터 저장 오류: {e}')
            return False
    
    def process_all_teams_annual_ranking(self, year: int = 2024):
        """모든 팀의 연말 랭킹 처리"""
        try:
            print(f"\n🚀 {year}년 모든 팀 연말 랭킹 생성 시작")
            
            # 1. 사용자 조직 매핑 가져오기
            user_org_mapping = self.get_user_organization_mapping()
            
            # 2. 연말 최종 점수 데이터 가져오기
            annual_scores = self.get_annual_final_scores(year)
            
            if not annual_scores:
                print("❌ 연말 점수 데이터가 없습니다.")
                return
            
            # 3. 조직 목록 가져오기
            organizations = {}
            for user_id, user_info in user_org_mapping.items():
                org_id = user_info['organization_id']
                org_name = user_info['organization_name']
                if org_id not in organizations:
                    organizations[org_id] = org_name
            
            print(f"📋 처리 대상: {len(organizations)}개 조직")
            print(f"📋 조직 목록: {organizations}")
            
            total_success = 0
            total_fail = 0
            
            # 4. 각 조직별로 처리
            for org_id, org_name in organizations.items():
                print(f"\n{'='*60}")
                print(f"📅 조직 {org_id} ({org_name}) 처리 시작")
                print(f"{'='*60}")
                
                # 연말 랭킹 생성
                annual_ranking = self.generate_team_annual_ranking(
                    org_id, org_name, year, user_org_mapping, annual_scores
                )
                
                if annual_ranking and self.save_ranking_data(annual_ranking):
                    total_success += 1
                else:
                    total_fail += 1
                
                # 분기별 랭킹 생성 (연말 보고서용)
                quarterly_rankings = self.generate_quarterly_rankings_for_annual(
                    org_id, org_name, year, user_org_mapping, annual_scores
                )
                
                for quarterly_ranking in quarterly_rankings:
                    if self.save_ranking_data(quarterly_ranking):
                        total_success += 1
                    else:
                        total_fail += 1
            
            print(f"\n🎉 전체 처리 완료!")
            print(f"✅ 총 성공: {total_success}개")
            print(f"❌ 총 실패: {total_fail}개")
            
        except Exception as e:
            print(f'❌ 전체 연말 랭킹 처리 오류: {e}')
            raise e
    
    def show_saved_annual_results(self, year: int = 2024):
        """저장된 연말 랭킹 결과 확인"""
        try:
            print(f"\n📊 {year}년 저장된 연말 랭킹 결과 요약")
            print("="*60)
            
            collection = self.mongo_db['ranking_results']
            
            # team-annual 타입 문서 조회
            annual_docs = list(collection.find({
                'type': 'team-annual',
                'evaluated_year': year
            }))
            
            # team-quarter 타입 문서 조회
            quarterly_docs = list(collection.find({
                'type': 'team-quarter',
                'evaluated_year': year
            }))
            
            print(f"📋 연말 랭킹: {len(annual_docs)}개")
            print(f"📋 분기별 랭킹: {len(quarterly_docs)}개")
            
            # 연말 랭킹 요약
            print(f"\n🏆 {year}년 연말 랭킹:")
            for doc in sorted(annual_docs, key=lambda x: x['organization_id']):
                org_id = doc['organization_id']
                org_name = doc['organization_name']
                member_count = doc['total_members']
                print(f"   조직 {org_id} ({org_name}): {member_count}명")
                
                # 1위 사용자 정보 출력
                if doc.get('team_ranking'):
                    top_member = doc['team_ranking'][0]
                    print(f"     1위: {top_member['name']} ({top_member['overall_final_score']}점)")
            
            # 분기별 랭킹 요약
            quarterly_by_org = {}
            for doc in quarterly_docs:
                org_id = doc['organization_id']
                if org_id not in quarterly_by_org:
                    quarterly_by_org[org_id] = []
                quarterly_by_org[org_id].append(doc)
            
            print(f"\n📅 분기별 랭킹 요약:")
            for org_id, docs in quarterly_by_org.items():
                org_name = docs[0]['organization_name']
                quarters = sorted([doc['evaluated_quarter'] for doc in docs])
                print(f"   조직 {org_id} ({org_name}): {quarters} 분기")
                
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
    system = AnnualTeamRankingSystem()
    
    try:
        system.connect_databases()
        
        # 2024년 모든 팀의 연말 랭킹 처리
        system.process_all_teams_annual_ranking(2024)
        
        # 저장된 결과 확인
        system.show_saved_annual_results(2024)
        
    except Exception as e:
        print(f'❌ 메인 처리 오류: {e}')
        import traceback
        traceback.print_exc()
    finally:
        system.disconnect_databases()

if __name__ == '__main__':
    main()