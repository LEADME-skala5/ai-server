import os
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict, Counter
import pymongo
import pymysql
from openai import OpenAI
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

class TeamNegativeKeywordAnalyzer:
    def __init__(self):
        self.maria_connection = None
        self.mongo_client = None
        self.mongo_db = None
        self.openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
    async def connect_databases(self):
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
            
            self.mongo_client = pymongo.MongoClient(
                mongo_url,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            
            # 연결 테스트
            self.mongo_client.admin.command('ping')
            self.mongo_db = self.mongo_client[os.getenv('MONGO_DB_NAME')]
            print('✅ MongoDB 연결 성공')
            
        except Exception as e:
            print(f'❌ 데이터베이스 연결 오류: {e}')
            raise e
    
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
    
    def get_user_organization_mapping(self) -> Dict[int, int]:
        """사용자 ID와 조직 ID 매핑 조회"""
        try:
            with self.maria_connection.cursor(pymysql.cursors.DictCursor) as cursor:
                query = """
                    SELECT id as user_id, organization_id
                    FROM users
                    WHERE organization_id IS NOT NULL
                    ORDER BY organization_id, id
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                
            # user_id -> organization_id 매핑
            user_org_mapping = {}
            org_user_count = defaultdict(int)
            
            for row in rows:
                user_id = row['user_id']
                org_id = row['organization_id']
                user_org_mapping[user_id] = org_id
                org_user_count[org_id] += 1
            
            print(f'👥 사용자-조직 매핑 조회 완료: {len(user_org_mapping)}명')
            for org_id, count in org_user_count.items():
                print(f"   조직 {org_id}: {count}명")
            
            return user_org_mapping
            
        except Exception as e:
            print(f'❌ 사용자-조직 매핑 조회 오류: {e}')
            raise e
    
    def get_organization_names(self) -> Dict[int, str]:
        """조직 ID와 이름 매핑 조회"""
        try:
            with self.maria_connection.cursor(pymysql.cursors.DictCursor) as cursor:
                query = """
                    SELECT division_id, name
                    FROM organizations
                    WHERE division_id IS NOT NULL
                    GROUP BY division_id, name
                    ORDER BY division_id
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                
            org_name_mapping = {}
            for row in rows:
                division_id = row['division_id']
                org_name = row['name']
                org_name_mapping[division_id] = org_name
            
            print(f'🏢 조직 이름 매핑 조회 완료: {len(org_name_mapping)}개')
            for org_id, name in org_name_mapping.items():
                print(f"   조직 {org_id}: {name}")
            
            return org_name_mapping
            
        except Exception as e:
            print(f'❌ 조직 이름 매핑 조회 오류: {e}')
            return {}
    
    def get_peer_evaluation_results(self, user_ids: List[int], evaluated_year: int, evaluated_quarter: int) -> List[Dict]:
        """특정 사용자들의 동료 평가 결과 조회"""
        try:
            peer_collection = self.mongo_db['peer_evaluation_results']
            
            print(f"🔍 {evaluated_year}년 {evaluated_quarter}분기 집계 문서 조회...")
            
            # 해당 연도/분기의 집계 문서 조회
            aggregate_doc = peer_collection.find_one({
                'type': 'personal-quarter',
                'evaluated_year': evaluated_year,
                'evaluated_quarter': evaluated_quarter
            })
            
            if not aggregate_doc:
                print(f"❌ {evaluated_year}년 {evaluated_quarter}분기 집계 문서를 찾을 수 없습니다.")
                return []
            
            print(f"✅ 집계 문서 발견: {aggregate_doc.get('user_count', 0)}명의 사용자 데이터")
            
            # users 배열에서 해당 user_ids에 해당하는 사용자들만 필터링
            all_users = aggregate_doc.get('users', [])
            filtered_users = []
            
            for user_data in all_users:
                user_id = user_data.get('user_id')
                if user_id in user_ids:
                    # keyword_summary.negative가 있는지 확인
                    keyword_summary = user_data.get('keyword_summary', {})
                    negative_keywords = keyword_summary.get('negative', [])
                    
                    if negative_keywords:  # 부정적 키워드가 있는 경우만
                        filtered_users.append(user_data)
            
            print(f"📋 조건에 맞는 사용자 데이터: {len(filtered_users)}명")
            
            if filtered_users:
                total_negative_keywords = sum(
                    len(user.get('keyword_summary', {}).get('negative', [])) 
                    for user in filtered_users
                )
                print(f"🔍 총 부정적 키워드 항목 수: {total_negative_keywords}개")
            
            return filtered_users
            
        except Exception as e:
            print(f'❌ 동료 평가 결과 조회 오류: {e}')
            raise e
    
    def analyze_negative_keywords(self, peer_results: List[Dict]) -> List[Dict]:
        """부정적 키워드 분석 및 상위 5개 추출"""
        try:
            keyword_counter = Counter()
            
            # 모든 문서에서 부정적 키워드 수집
            for result in peer_results:
                negative_keywords = result.get('keyword_summary', {}).get('negative', [])
                
                for keyword_data in negative_keywords:
                    if isinstance(keyword_data, dict):
                        keyword = keyword_data.get('keyword', '')
                        count = keyword_data.get('count', 0)
                        if keyword:
                            keyword_counter[keyword] += count
            
            # 상위 5개 키워드 추출
            top_5_keywords = [
                {'keyword': keyword, 'count': count}
                for keyword, count in keyword_counter.most_common(5)
            ]
            
            print(f'🔍 부정적 키워드 분석 완료: 총 {len(keyword_counter)}개 키워드, 상위 5개 추출')
            for i, item in enumerate(top_5_keywords, 1):
                print(f"   {i}. {item['keyword']}: {item['count']}회")
            
            return top_5_keywords
            
        except Exception as e:
            print(f'❌ 부정적 키워드 분석 오류: {e}')
            raise e
    
    async def generate_improvement_recommendations(self, org_name: str, top_keywords: List[Dict], evaluated_year: int, evaluated_quarter: int) -> str:
        """GPT-4o를 사용한 개선 제언 생성"""
        try:
            if not top_keywords:
                return "분석할 부정적 키워드가 없어 제언을 생성할 수 없습니다."
            
            keywords_text = ', '.join([f"{item['keyword']}({item['count']}회)" for item in top_keywords])
            
            prompt = f"""
{org_name} 조직의 {evaluated_year}년 {evaluated_quarter}분기 동료 평가에서 나타난 주요 부정적 키워드 분석 결과입니다.

상위 5개 부정적 키워드:
{chr(10).join([f"{item['keyword']}: {item['count']}회 언급" for item in top_keywords])}

위의 부정적 키워드 분석 결과를 바탕으로 {org_name} 조직의 개선 방안을 제시해주세요.

각 키워드별 구체적인 원인 분석과 실행 가능한 개선 방안을 제시하되, 단기적 해결책과 장기적 개선 전략을 모두 포함해주세요. 또한 조직 문화 개선을 위한 구체적인 액션 플랜도 함께 제안해주세요.

번호나 기호 없이 자연스러운 문단 형태의 일반 텍스트로만 작성해주세요.
            """
            
            print(f'🤖 {org_name} 조직 개선 제언 생성 중...')
            
            response = self.openai_client.chat.completions.create(
                model='gpt-4o',
                messages=[
                    {
                        'role': 'system',
                        'content': '당신은 조직 문화 및 인사 관리 전문가입니다. 동료 평가에서 나타난 부정적 키워드를 분석하여 조직의 실질적인 개선 방안을 제시해주세요. 구체적이고 실행 가능한 솔루션을 중심으로 작성하되, 번호나 기호 없이 자연스러운 문단 형태로 작성해주세요.'
                    },
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ],
                temperature=0.7,
                max_tokens=2000
            )
            
            recommendations = response.choices[0].message.content
            print(f'✅ {org_name} 조직 개선 제언 생성 완료')
            
            return recommendations
            
        except Exception as e:
            print(f'❌ {org_name} 조직 개선 제언 생성 오류: {e}')
            raise e
    
    def save_team_quarter_analysis(self, data: Dict) -> bool:
        """팀 분기별 분석 결과 저장"""
        try:
            collection = self.mongo_db['peer_evaluation_results']
            
            document = {
                'type': 'team_quarter',
                'organization': data['organization_id'],
                'organization_name': data.get('organization_name', ''),
                'evaluated_year': data['evaluated_year'],
                'evaluated_quarter': data['evaluated_quarter'],
                'analysis_summary': {
                    'total_members_analyzed': data['total_members'],
                    'total_negative_keywords': len(data['top_keywords']),
                    'total_mentions': sum(item['count'] for item in data['top_keywords'])
                },
                'top_negative_keywords': data['top_keywords'],
                'improvement_recommendations': data['recommendations'],
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            # 기존 데이터가 있으면 업데이트, 없으면 삽입
            filter_query = {
                'type': 'team_quarter',
                'organization': data['organization_id'],
                'evaluated_year': data['evaluated_year'],
                'evaluated_quarter': data['evaluated_quarter']
            }
            
            result = collection.replace_one(filter_query, document, upsert=True)
            
            if result.upserted_id:
                print(f'✅ 조직 {data["organization_id"]} 팀 분기 분석 결과 신규 저장')
            else:
                print(f'✅ 조직 {data["organization_id"]} 팀 분기 분석 결과 업데이트 완료')
            
            return True
            
        except Exception as e:
            print(f'❌ 조직 {data["organization_id"]} 팀 분기 분석 결과 저장 오류: {e}')
            return False
    
    async def analyze_organization_quarter(self, org_id: int, org_name: str, user_ids: List[int], evaluated_year: int, evaluated_quarter: int) -> bool:
        """특정 조직의 특정 분기 분석"""
        try:
            print(f'\n🔄 {org_name} 조직 {evaluated_year}년 {evaluated_quarter}분기 분석 시작 ({len(user_ids)}명)')
            
            # 1. 동료 평가 결과 조회
            peer_results = self.get_peer_evaluation_results(user_ids, evaluated_year, evaluated_quarter)
            
            if not peer_results:
                print(f'⚠️ {org_name} 조직 {evaluated_quarter}분기 동료 평가 데이터가 없습니다.')
                return False
            
            # 2. 부정적 키워드 분석
            top_keywords = self.analyze_negative_keywords(peer_results)
            
            if not top_keywords:
                print(f'⚠️ {org_name} 조직 {evaluated_quarter}분기 부정적 키워드가 없습니다.')
                return False
            
            # 3. GPT 개선 제언 생성
            recommendations = await self.generate_improvement_recommendations(
                org_name, top_keywords, evaluated_year, evaluated_quarter
            )
            
            # 4. 결과 저장
            save_result = self.save_team_quarter_analysis({
                'organization_id': org_id,
                'organization_name': org_name,
                'evaluated_year': evaluated_year,
                'evaluated_quarter': evaluated_quarter,
                'total_members': len(user_ids),
                'top_keywords': top_keywords,
                'recommendations': recommendations
            })
            
            if save_result:
                print(f'✅ {org_name} 조직 {evaluated_quarter}분기 분석 완료')
                return True
            else:
                print(f'❌ {org_name} 조직 {evaluated_quarter}분기 저장 실패')
                return False
            
        except Exception as e:
            print(f'❌ {org_name} 조직 {evaluated_quarter}분기 분석 오류: {e}')
            return False
    
    async def analyze_all_organizations_quarters(self, evaluated_year: int):
        """모든 조직의 모든 분기 분석 (None 분기 제외)"""
        try:
            print(f'\n🚀 {evaluated_year}년 팀 단위 부정적 키워드 분석 시작')
            
            await self.connect_databases()
            
            # 1. 사용자-조직 매핑 조회
            user_org_mapping = self.get_user_organization_mapping()
            
            # 2. 조직 이름 매핑 조회
            org_name_mapping = self.get_organization_names()
            
            # 3. 조직별 사용자 그룹화
            org_users = defaultdict(list)
            for user_id, org_id in user_org_mapping.items():
                org_users[org_id].append(user_id)
            
            success_count = 0
            fail_count = 0
            
            # 4. 각 조직별, 분기별 분석 (None 분기 제외)
            for org_id, user_ids in org_users.items():
                org_name = org_name_mapping.get(org_id, f'조직{org_id}')
                
                # 1-4분기만 처리 (None 분기 제외)
                for quarter in [1, 2, 3, 4]:
                    result = await self.analyze_organization_quarter(
                        org_id, org_name, user_ids, evaluated_year, quarter
                    )
                    
                    if result:
                        success_count += 1
                    else:
                        fail_count += 1
                    
                    # API 호출 제한 고려 지연
                    await asyncio.sleep(1)
            
            print(f'\n🎉 팀 단위 부정적 키워드 분석 완료!')
            print(f'✅ 성공: {success_count}개 조직-분기')
            print(f'❌ 실패: {fail_count}개 조직-분기')
            
        except Exception as e:
            print(f'❌ 전체 분석 처리 오류: {e}')
            raise e
        finally:
            self.disconnect_databases()


# 사용 예시 및 실행부
async def main():
    analyzer = TeamNegativeKeywordAnalyzer()
    
    try:
        # 2024년 전체 조직 분석
        await analyzer.analyze_all_organizations_quarters(2024)
        
    except Exception as e:
        print(f'❌ 메인 처리 오류: {e}')
        exit(1)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단됨")
    except Exception as e:
        print(f"❌ 실행 오류: {e}")
        exit(1)