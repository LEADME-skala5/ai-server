import os
import asyncio
import math
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
import pymongo
import pymysql
from openai import OpenAI
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

class TeamQuarterlyEvaluationSystem:
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
            
            # MongoDB 연결 (인증 옵션 추가)
            mongo_url = f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/{os.getenv('MONGO_DB_NAME')}?authSource=admin"
            
            print(f"🔍 MongoDB 연결 시도: {mongo_url.replace(os.getenv('MONGO_PASSWORD'), '***')}")
            
            self.mongo_client = pymongo.MongoClient(
                mongo_url,
                serverSelectionTimeoutMS=5000,  # 5초 타임아웃
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            
            # 연결 테스트
            self.mongo_client.admin.command('ping')
            self.mongo_db = self.mongo_client[os.getenv('MONGO_DB_NAME')]
            print('✅ MongoDB 연결 성공')
            
        except Exception as e:
            print(f'❌ 데이터베이스 연결 오류: {e}')
            
            # MongoDB 연결 대안 시도
            try:
                print("🔄 MongoDB 인증 없이 연결 시도...")
                mongo_url_no_auth = f"mongodb://{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/{os.getenv('MONGO_DB_NAME')}"
                self.mongo_client = pymongo.MongoClient(mongo_url_no_auth)
                self.mongo_client.admin.command('ping')
                self.mongo_db = self.mongo_client[os.getenv('MONGO_DB_NAME')]
                print('✅ MongoDB 연결 성공 (인증 없음)')
            except Exception as e2:
                print(f'❌ MongoDB 인증 없는 연결도 실패: {e2}')
                raise e
    
    def get_organization_names(self) -> Dict[int, str]:
        """organizations 테이블에서 division_id와 name 매핑 조회"""
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
                
            # division_id -> name 매핑 딕셔너리 생성
            org_name_mapping = {}
            for row in rows:
                division_id = row['division_id']
                org_name = row['name']
                org_name_mapping[division_id] = org_name
            
            print(f'🏢 조직 이름 매핑 조회 완료: {len(org_name_mapping)}개')
            for div_id, name in org_name_mapping.items():
                print(f"   조직 {div_id}: {name}")
            
            return org_name_mapping
            
        except Exception as e:
            print(f'❌ 조직 이름 매핑 조회 오류: {e}')
            return {}
    
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
    
    def get_available_quarters(self) -> List[tuple]:
        """처리 가능한 모든 분기 조회 (personal-quarter 타입 기준)"""
        try:
            reports_collection = self.mongo_db['reports']
            pipeline = [
                {"$match": {"type": "personal-quarter", "rank.organization_id": {"$exists": True}}},
                {"$group": {
                    "_id": {
                        "year": "$evaluated_year", 
                        "quarter": "$evaluated_quarter"
                    }
                }},
                {"$sort": {"_id.year": 1, "_id.quarter": 1}}
            ]
            
            quarters = list(reports_collection.aggregate(pipeline))
            quarter_list = [(q['_id']['year'], q['_id']['quarter']) for q in quarters]
            
            print(f"📅 처리 가능한 분기: {quarter_list}")
            return quarter_list
            
        except Exception as e:
            print(f'❌ 분기 데이터 조회 오류: {e}')
            return []
    
    def get_reports_by_organization(self, evaluated_year: int, evaluated_quarter: int) -> Dict[str, List[Dict]]:
        """organization_id별로 personal-quarter 보고서 조회"""
        try:
            reports_collection = self.mongo_db['reports']
            
            print(f"🔍 {evaluated_year}년 {evaluated_quarter}분기 MongoDB reports 컬렉션 데이터 조회...")
            
            # 수정된 쿼리 (rank.organization_id 사용)
            query = {
                'type': 'personal-quarter',
                'evaluated_year': evaluated_year,
                'evaluated_quarter': evaluated_quarter,
                'rank.organization_id': {'$exists': True, '$ne': None}
            }
            
            print(f"🎯 쿼리: {query}")
            reports = list(reports_collection.find(query))
            
            print(f'📋 {len(reports)}개의 개인 분기별 보고서 조회 완료')
            
            if len(reports) == 0:
                print(f"❌ {evaluated_year}년 {evaluated_quarter}분기 데이터가 없습니다.")
                return {}
            
            # rank.organization_id별로 그룹화
            org_reports = {}
            for report in reports:
                # rank 안에서 organization_id 추출
                org_id = str(report.get('rank', {}).get('organization_id', 'unknown'))
                if org_id not in org_reports:
                    org_reports[org_id] = []
                org_reports[org_id].append(report)
            
            print(f'🏢 총 {len(org_reports)}개 조직 발견')
            
            # 각 조직별 보고서 수 출력
            for org_id, reports_list in org_reports.items():
                print(f"🔢 조직 {org_id}: {len(reports_list)}개 보고서")
                if reports_list:
                    first_report = reports_list[0]
                    user_name = first_report.get('user', {}).get('name', 'Unknown')
                    final_score = first_report.get('finalScore', 0)
                    print(f"   예시 - {user_name}: {final_score}점")
            
            return org_reports
            
        except Exception as e:
            print(f'❌ 조직별 보고서 조회 오류: {e}')
            raise e
    
    def classify_division_performance(self, reports: List[Dict]) -> Tuple[List[Dict], List[Dict], int]:
        """조직 내에서 개인별 finalScore 기준 상위/하위 20% 분류"""
        if not reports:
            return [], [], 0
        
        # finalScore 기준으로 내림차순 정렬 (높은 점수가 상위)
        sorted_reports = sorted(reports, key=lambda x: x.get('finalScore', 0), reverse=True)
        
        total_count = len(sorted_reports)
        top_20_percent_count = math.ceil(total_count * 0.2)
        bottom_20_percent_count = math.ceil(total_count * 0.2)
        
        # 상위 20% (점수가 높은 순)
        top_performers = sorted_reports[:top_20_percent_count]
        
        # 하위 20% (점수가 낮은 순)
        bottom_performers = sorted_reports[-bottom_20_percent_count:]
        
        print(f'🎯 총 {total_count}명 중 상위 {top_20_percent_count}명, 하위 {bottom_20_percent_count}명 분류 완료')
        
        return top_performers, bottom_performers, total_count
    
    async def generate_division_management_strategy(self, top_performers: List[Dict], bottom_performers: List[Dict], org_name: str) -> str:
        """GPT-4o를 사용한 조직별 맞춤 관리 방향 생성"""
        try:
            # 상위 성과자들의 finalComment 수집
            top_comments = [
                report.get('finalComment', '') 
                for report in top_performers 
                if report.get('finalComment', '').strip()
            ]
            
            # 하위 성과자들의 finalComment 수집
            bottom_comments = [
                report.get('finalComment', '') 
                for report in bottom_performers 
                if report.get('finalComment', '').strip()
            ]
            
            # 점수 정보
            top_scores = [p.get('finalScore', 0) for p in top_performers]
            bottom_scores = [p.get('finalScore', 0) for p in bottom_performers]
            
            top_avg_score = sum(top_scores) / len(top_scores) if top_scores else 0
            bottom_avg_score = sum(bottom_scores) / len(bottom_scores) if bottom_scores else 0
            
            prompt = f"""
{org_name} 조직의 분기별 성과 평가 분석 결과입니다.

상위 20% 성과자 ({len(top_performers)}명)
평균 점수: {top_avg_score:.1f}점
최종 코멘트들:
{chr(10).join([f'{i+1}. {comment}' for i, comment in enumerate(top_comments)])}

하위 20% 성과자 ({len(bottom_performers)}명)
평균 점수: {bottom_avg_score:.1f}점
최종 코멘트들:
{chr(10).join([f'{i+1}. {comment}' for i, comment in enumerate(bottom_comments)])}

위의 성과 분석 결과를 바탕으로 {org_name} 조직에 특화된 관리 전략을 작성해주세요.

상위 성과자 관리 전략부터 시작해서 강점 유지 및 확산 방안, 동기부여 및 성장 지원 방법, 멘토링 역할 활용 방안을 설명하고, 이어서 하위 성과자 개선 전략으로 핵심 개선 포인트 및 원인 분석, 구체적인 역량 개발 계획, 단계별 성과 향상 로드맵을 제시하며, 마지막으로 조직 전체 발전 방향으로 조직 내 성과 격차 해소 방안, 협업 및 지식 공유 활성화, 장기적 조직 역량 강화 전략을 다뤄주세요.

각 내용은 구체적이고 실행 가능한 액션 플랜으로 작성하되, 번호나 기호(1., 2., -, *, #, ## 등) 없이 자연스러운 문단 형태의 일반 텍스트로만 작성해주세요.
            """
            
            print(f'🤖 {org_name} 조직 관리 전략 생성 중...')
            
            response = self.openai_client.chat.completions.create(
                model='gpt-4o',
                messages=[
                    {
                        'role': 'system',
                        'content': '당신은 조직 관리 및 인사 전문가입니다. 조직별 성과 분석 결과를 바탕으로 해당 조직에 특화된 실무적이고 구체적인 관리 전략을 제시해주세요. 일반론보다는 제시된 데이터의 특성을 반영한 맞춤형 솔루션을 제공하는 것이 중요합니다. 번호, 기호, 마크다운 문법을 일절 사용하지 말고 자연스러운 문단 형태의 일반 텍스트로만 작성해주세요.'
                    },
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ],
                temperature=0.7,
                max_tokens=2500
            )
            
            management_strategy = response.choices[0].message.content
            print(f'✅ {org_name} 조직 관리 전략 생성 완료')
            
            return management_strategy
            
        except Exception as e:
            print(f'❌ {org_name} 조직 GPT 응답 생성 오류: {e}')
            raise e
    
    def save_division_strategic_observation(self, data: Dict) -> bool:
        """조직별 전략적 관찰 결과 MongoDB에 저장"""
        try:
            collection = self.mongo_db['team_strategic_observations']
            
            document = {
                'division_id': data['division_id'],
                'division_name': data.get('division_name', ''),  # 조직 이름 추가
                'evaluated_year': data['evaluated_year'],
                'evaluated_quarter': data['evaluated_quarter'],
                'analysis_summary': {
                    'total_members': data['total_members'],
                    'top_performers_count': data['top_performers_count'],
                    'bottom_performers_count': data['bottom_performers_count'],
                    'top_performers_avg_score': data['top_avg_score'],
                    'bottom_performers_avg_score': data['bottom_avg_score']
                },
                'top_performers': [
                    {
                        'user_id': p.get('user', {}).get('userId'),
                        'user_name': p.get('user', {}).get('name'),
                        'finalScore': p.get('finalScore', 0),
                        'finalComment': p.get('finalComment', '')
                    } for p in data['top_performers']
                ],
                'bottom_performers': [
                    {
                        'user_id': p.get('user', {}).get('userId'),
                        'user_name': p.get('user', {}).get('name'),
                        'finalScore': p.get('finalScore', 0),
                        'finalComment': p.get('finalComment', '')
                    } for p in data['bottom_performers']
                ],
                'management_strategy': data['management_strategy'],
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            # 기존 데이터가 있으면 업데이트, 없으면 삽입
            filter_query = {
                'division_id': data['division_id'],
                'evaluated_year': data['evaluated_year'],
                'evaluated_quarter': data['evaluated_quarter']
            }
            
            result = collection.replace_one(filter_query, document, upsert=True)
            
            if result.upserted_id:
                print(f'✅ 조직 {data["division_id"]} 전략적 관찰 결과 신규 저장: {result.upserted_id}')
            else:
                print(f'✅ 조직 {data["division_id"]} 전략적 관찰 결과 업데이트 완료')
            
            return True
            
        except Exception as e:
            print(f'❌ 조직 {data["division_id"]} 전략적 관찰 결과 저장 오류: {e}')
            return False
    
    async def process_organization_evaluation(self, org_id: str, org_name: str, reports: List[Dict], evaluated_year: int, evaluated_quarter: int) -> bool:
        """특정 조직의 분기별 평가 처리"""
        try:
            print(f'\n🔄 {org_name} 조직 처리 시작 ({len(reports)}개 보고서)')
            
            if not reports:
                print(f'⚠️ {org_name} 조직: {evaluated_year}년 {evaluated_quarter}분기 보고서가 없습니다.')
                return False
            
            # 1. 조직 내에서 개인별 finalScore 기준으로 상위/하위 20% 분류
            top_performers, bottom_performers, total_count = self.classify_division_performance(reports)
            
            # 평균 점수 계산
            top_avg_score = sum(p.get('finalScore', 0) for p in top_performers) / len(top_performers) if top_performers else 0
            bottom_avg_score = sum(p.get('finalScore', 0) for p in bottom_performers) / len(bottom_performers) if bottom_performers else 0
            
            print(f'📊 {org_name} 조직 분석 결과:')
            print(f'   - 상위 20%: {len(top_performers)}명 (평균 {top_avg_score:.1f}점)')
            print(f'   - 하위 20%: {len(bottom_performers)}명 (평균 {bottom_avg_score:.1f}점)')
            
            # 2. GPT를 통한 조직별 맞춤 관리 전략 생성
            management_strategy = await self.generate_division_management_strategy(
                top_performers, 
                bottom_performers, 
                org_name
            )
            
            # 3. 결과를 MongoDB에 저장 (org_id와 org_name 모두 저장)
            save_result = self.save_division_strategic_observation({
                'division_id': org_id,  # organization_id를 division_id로 사용
                'division_name': org_name,  # 조직 이름 추가
                'evaluated_year': evaluated_year,
                'evaluated_quarter': evaluated_quarter,
                'total_members': total_count,
                'top_performers_count': len(top_performers),
                'bottom_performers_count': len(bottom_performers),
                'top_avg_score': top_avg_score,
                'bottom_avg_score': bottom_avg_score,
                'top_performers': top_performers,
                'bottom_performers': bottom_performers,
                'management_strategy': management_strategy
            })
            
            if save_result:
                print(f'✅ {org_name} 조직 처리 완료')
                return True
            else:
                print(f'❌ {org_name} 조직 저장 실패')
                return False
            
        except Exception as e:
            print(f'❌ {org_name} 조직 처리 오류: {e}')
            import traceback
            traceback.print_exc()
            return False
    
    async def process_all_organizations_all_quarters(self):
        """모든 조직의 모든 분기 평가 처리 (메인 함수)"""
        try:
            print(f'\n🚀 모든 조직 모든 분기 팀 단위 전략적 관찰 생성 시작')
            
            await self.connect_databases()
            
            # 1. 조직 이름 매핑 조회
            org_name_mapping = self.get_organization_names()
            
            # 2. 처리 가능한 모든 분기 조회
            available_quarters = self.get_available_quarters()
            
            if not available_quarters:
                print("⚠️ 처리할 분기 데이터가 없습니다.")
                return
            
            total_success = 0
            total_fail = 0
            
            # 3. 각 분기별로 모든 조직 처리
            for year, quarter in available_quarters:
                print(f'\n{"="*60}')
                print(f'📅 {year}년 {quarter}분기 처리 시작')
                print(f'{"="*60}')
                
                # organization_id별로 보고서 조회
                org_reports = self.get_reports_by_organization(year, quarter)
                
                if not org_reports:
                    print(f"⚠️ {year}년 {quarter}분기 조직별 보고서가 없습니다.")
                    continue
                
                org_ids = list(org_reports.keys())
                print(f'📋 처리 대상 조직: {", ".join([f"{org_id}({org_name_mapping.get(int(org_id), org_id)})" for org_id in org_ids])}')
                
                quarter_success = 0
                quarter_fail = 0
                
                # 각 조직별로 순차적으로 처리
                for org_id, reports in org_reports.items():
                    org_name = org_name_mapping.get(int(org_id), f'조직{org_id}')
                    result = await self.process_organization_evaluation(
                        org_id, 
                        org_name,
                        reports, 
                        year, 
                        quarter
                    )
                    
                    if result:
                        quarter_success += 1
                        total_success += 1
                    else:
                        quarter_fail += 1
                        total_fail += 1
                    
                    # OpenAI API 호출 제한을 고려한 지연 (1초)
                    await asyncio.sleep(1)
                
                print(f'📊 {year}년 {quarter}분기 결과: 성공 {quarter_success}개, 실패 {quarter_fail}개')
            
            print(f'\n🎉 모든 분기 팀 단위 전략적 관찰 생성 완료!')
            print(f'✅ 총 성공: {total_success}개')
            print(f'❌ 총 실패: {total_fail}개')
            
        except Exception as e:
            print(f'❌ 전체 평가 처리 오류: {e}')
            raise e
        finally:
            self.disconnect_databases()

    async def process_all_organizations_evaluation(self, evaluated_year: int, evaluated_quarter: int):
        """특정 분기의 모든 조직 평가 처리 (단일 분기용)"""
        try:
            print(f'\n🚀 {evaluated_year}년 {evaluated_quarter}분기 팀 단위 분기 보고서 생성 시작')
            
            await self.connect_databases()
            
            # 1. 조직 이름 매핑 조회
            org_name_mapping = self.get_organization_names()
            
            # 2. organization_id별로 보고서 조회
            org_reports = self.get_reports_by_organization(evaluated_year, evaluated_quarter)
            
            if not org_reports:
                print("⚠️ 처리할 조직별 보고서가 없습니다.")
                return
            
            org_ids = list(org_reports.keys())
            print(f'📋 처리 대상 조직: {", ".join([f"{org_id}({org_name_mapping.get(int(org_id), org_id)})" for org_id in org_ids])}')
            
            success_count = 0
            fail_count = 0
            
            # 3. 각 조직별로 순차적으로 처리
            for org_id, reports in org_reports.items():
                org_name = org_name_mapping.get(int(org_id), f'조직{org_id}')
                result = await self.process_organization_evaluation(
                    org_id, 
                    org_name,
                    reports, 
                    evaluated_year, 
                    evaluated_quarter
                )
                
                if result:
                    success_count += 1
                else:
                    fail_count += 1
                
                # OpenAI API 호출 제한을 고려한 지연 (1초)
                await asyncio.sleep(1)
            
            print(f'\n🎉 팀 단위 분기 보고서 생성 완료!')
            print(f'✅ 성공: {success_count}개 조직')
            print(f'❌ 실패: {fail_count}개 조직')
            
        except Exception as e:
            print(f'❌ 전체 연말 평가 처리 오류: {e}')
            raise e
        finally:
            self.disconnect_databases()


# 사용 예시 및 실행부
async def main():
    evaluation_system = TeamQuarterlyEvaluationSystem()
    
    try:
        # 모든 조직의 모든 분기 처리 (개선된 버전)
        await evaluation_system.process_all_organizations_all_quarters()
        
        # 또는 특정 분기만 처리하려면:
        # await evaluation_system.process_all_organizations_evaluation(2024, 4)
        
    except Exception as e:
        print(f'❌ 메인 처리 오류: {e}')
        exit(1)


if __name__ == '__main__':
    # Python 3.11.9에서 asyncio 실행
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단됨")
    except Exception as e:
        print(f"❌ 실행 오류: {e}")
        exit(1)