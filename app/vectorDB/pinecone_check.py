#!/usr/bin/env python3
"""
완전한 Pinecone 데이터 분석기
모든 벡터 데이터를 조회하고 분석하는 도구
"""

import json
from pinecone import Pinecone
from typing import Dict, List, Any, Optional
import pandas as pd
from datetime import datetime
import time

class CompletePineconeAnalyzer:
    def __init__(self, api_key: str, index_name: str = "skore-20250624-144422"):
        """
        완전한 Pinecone 데이터 분석기 초기화
        """
        self.pc = Pinecone(api_key=api_key)
        self.index_name = index_name
        self.index = self.pc.Index(index_name)
        
        # 네임스페이스 자동 감지
        try:
            stats = self.index.describe_index_stats()
            available_namespaces = list(stats.namespaces.keys()) if stats.namespaces else [""]
            self.namespace = available_namespaces[0] if available_namespaces and available_namespaces[0] != "" else ""
            print(f"🔌 Pinecone 연결 완료 - 인덱스: {index_name}")
            print(f"🎯 사용할 네임스페이스: '{self.namespace}'")
        except Exception as e:
            print(f"⚠️ 네임스페이스 감지 실패: {e}")
            self.namespace = ""
    
    def get_complete_data_analysis(self) -> Dict[str, Any]:
        """전체 데이터를 완전히 분석합니다."""
        print(f"\n🔍 === 완전한 데이터 분석 시작 ===")
        
        # 1. 인덱스 통계
        stats = self.index.describe_index_stats()
        total_vectors = stats.total_vector_count
        print(f"📊 총 벡터 수: {total_vectors}")
        
        # 2. 전체 데이터 수집 (여러 번 쿼리)
        all_data = self.collect_all_vectors(total_vectors)
        print(f"✅ 실제 수집된 벡터 수: {len(all_data)}")
        
        # 3. 메타데이터 분석
        analysis = self.analyze_complete_metadata(all_data)
        
        return analysis
    
    def collect_all_vectors(self, total_count: int, batch_size: int = 1000) -> List[Dict[str, Any]]:
        """모든 벡터를 배치로 수집합니다."""
        print(f"\n📦 전체 {total_count}개 벡터 수집 중... (배치 크기: {batch_size})")
        
        all_vectors = []
        collected_ids = set()
        dummy_vector = [0.0] * 1024
        
        # 여러 번 쿼리하여 모든 데이터 수집
        iterations = 0
        max_iterations = 10  # 무한루프 방지
        
        while len(all_vectors) < total_count and iterations < max_iterations:
            iterations += 1
            print(f"🔄 {iterations}차 수집 중... (현재: {len(all_vectors)}/{total_count})")
            
            try:
                query_params = {
                    "vector": dummy_vector,
                    "top_k": min(batch_size, 10000),  # Pinecone 최대 제한
                    "include_metadata": True
                }
                
                if self.namespace:
                    query_params["namespace"] = self.namespace
                
                results = self.index.query(**query_params)
                
                new_vectors = 0
                for match in results.matches:
                    if match.id not in collected_ids:
                        all_vectors.append({
                            "id": match.id,
                            "score": match.score,
                            "metadata": match.metadata
                        })
                        collected_ids.add(match.id)
                        new_vectors += 1
                
                print(f"   새로 수집된 벡터: {new_vectors}개")
                
                if new_vectors == 0:
                    print(f"   더 이상 새로운 벡터가 없음, 수집 완료")
                    break
                
                # API 레이트 리미트 방지
                time.sleep(0.1)
                
            except Exception as e:
                print(f"❌ {iterations}차 수집 실패: {e}")
                break
        
        return all_vectors
    
    def analyze_complete_metadata(self, all_data: List[Dict]) -> Dict[str, Any]:
        """모든 메타데이터를 완전히 분석합니다."""
        print(f"\n📊 메타데이터 완전 분석 중... ({len(all_data)}개 벡터)")
        
        if not all_data:
            return {}
        
        # 메타데이터 수집
        all_keys = set()
        user_ids = set()
        organizations = set()
        source_files = set()
        years = set()
        quarters = set()
        
        # 샘플 데이터 저장
        sample_records = []
        
        for item in all_data:
            metadata = item.get("metadata", {})
            all_keys.update(metadata.keys())
            
            # 각 필드별 고유값 수집
            if "user_id" in metadata:
                user_ids.add(str(metadata["user_id"]))
            if "organization_id" in metadata:
                organizations.add(str(metadata["organization_id"]))
            if "source_file" in metadata:
                source_files.add(str(metadata["source_file"]))
            if "evaluation_year" in metadata:
                years.add(str(metadata["evaluation_year"]))
            if "evaluation_quarter" in metadata:
                quarters.add(str(metadata["evaluation_quarter"]))
            
            # 샘플 레코드 저장 (각 user_id별로 1개씩)
            if len(sample_records) < 20:
                sample_records.append({
                    "id": item["id"],
                    "user_id": metadata.get("user_id"),
                    "organization_id": metadata.get("organization_id"), 
                    "start_date": metadata.get("start_date"),
                    "end_date": metadata.get("end_date"),
                    "done_task_preview": metadata.get("done_task", "")[:150] + "..."
                })
        
        analysis = {
            "total_vectors": len(all_data),
            "metadata_keys": sorted(list(all_keys)),
            "unique_user_ids": sorted(list(user_ids)),
            "unique_organizations": sorted(list(organizations)),
            "source_files": sorted(list(source_files)),
            "evaluation_years": sorted(list(years)),
            "evaluation_quarters": sorted(list(quarters)),
            "sample_records": sample_records
        }
        
        # 결과 출력
        print(f"\n📋 === 완전한 분석 결과 ===")
        print(f"총 벡터 수: {analysis['total_vectors']}")
        print(f"메타데이터 키: {len(analysis['metadata_keys'])}개")
        print(f"  {analysis['metadata_keys']}")
        print(f"\n👥 사용자 ID: {len(analysis['unique_user_ids'])}명")
        print(f"  {analysis['unique_user_ids']}")
        print(f"\n🏢 조직 ID: {len(analysis['unique_organizations'])}개")
        print(f"  {analysis['unique_organizations']}")
        print(f"\n📁 소스 파일: {len(analysis['source_files'])}개")
        print(f"  {analysis['source_files']}")
        print(f"\n📅 평가 년도: {analysis['evaluation_years']}")
        print(f"📅 평가 분기: {analysis['evaluation_quarters']}")
        
        # 각 사용자별 데이터 개수 계산
        user_data_counts = {}
        for item in all_data:
            user_id = item.get("metadata", {}).get("user_id")
            if user_id:
                user_data_counts[str(user_id)] = user_data_counts.get(str(user_id), 0) + 1
        
        print(f"\n📊 사용자별 데이터 개수:")
        for user_id in sorted(user_data_counts.keys()):
            print(f"  User {user_id}: {user_data_counts[user_id]}건")
        
        analysis["user_data_counts"] = user_data_counts
        
        return analysis
    
    def search_user_data_complete(self, user_id: str) -> List[Dict[str, Any]]:
        """특정 사용자의 모든 데이터를 검색합니다."""
        print(f"\n🎯 사용자 '{user_id}' 전체 데이터 검색...")
        
        try:
            dummy_vector = [0.0] * 1024
            
            # 해당 사용자의 모든 데이터 검색
            query_params = {
                "vector": dummy_vector,
                "filter": {"user_id": str(user_id)},
                "top_k": 10000,  # 충분히 큰 수
                "include_metadata": True
            }
            
            if self.namespace:
                query_params["namespace"] = self.namespace
            
            results = self.index.query(**query_params)
            
            user_data = []
            for match in results.matches:
                user_data.append({
                    "id": match.id,
                    "score": match.score,
                    "metadata": match.metadata
                })
            
            print(f"✅ 사용자 '{user_id}' 데이터 {len(user_data)}건 발견")
            
            # 데이터 상세 출력
            if user_data:
                print(f"\n📋 사용자 '{user_id}' 데이터 상세:")
                for i, item in enumerate(user_data, 1):
                    metadata = item["metadata"]
                    print(f"  {i}. 기간: {metadata.get('start_date')} ~ {metadata.get('end_date')}")
                    print(f"     업무: {metadata.get('done_task', '')[:100]}...")
                    print()
            
            return user_data
            
        except Exception as e:
            print(f"❌ 사용자 데이터 검색 실패: {e}")
            return []
    
    def export_complete_analysis(self, analysis: Dict[str, Any], filename: str = None) -> str:
        """완전한 분석 결과를 저장합니다."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"complete_pinecone_analysis_{timestamp}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(analysis, f, ensure_ascii=False, indent=2)
            
            print(f"💾 완전한 분석 결과 저장: {filename}")
            return filename
            
        except Exception as e:
            print(f"❌ 분석 결과 저장 실패: {e}")
            return ""


def main():
    """메인 실행 함수"""
    print("🔍 === 완전한 Pinecone 데이터 분석기 ===")
    print("모든 벡터 데이터를 수집하고 완전히 분석합니다.")
    
    # API 키 설정
    api_key = "pcsk_5Wcu2A_7QAdTAjfmSYxwxc2sfiZ7G1bhmi9qy6J2KXL1hUfNcoLQ3xGdavA7S4E9DEqpmH"
    index_name = "skore-20250624-144422"
    
    try:
        # 분석기 초기화
        analyzer = CompletePineconeAnalyzer(api_key, index_name)
        
        # 완전한 데이터 분석 실행
        analysis = analyzer.get_complete_data_analysis()
        
        if not analysis:
            print("❌ 분석할 데이터가 없습니다.")
            return
        
        # 사용자 선택 메뉴
        while True:
            print(f"\n🎯 === 추가 작업 선택 ===")
            print("1. 특정 사용자 전체 데이터 검색")
            print("2. 분석 결과 JSON 저장")
            print("3. 사용자별 데이터 통계 재출력")
            print("4. 종료")
            
            choice = input("\n선택하세요 (1-4): ").strip()
            
            if choice == "1":
                available_users = analysis.get('unique_user_ids', [])
                print(f"\n사용 가능한 사용자 ID: {available_users}")
                user_id = input("검색할 사용자 ID를 입력하세요: ").strip()
                if user_id:
                    analyzer.search_user_data_complete(user_id)
            
            elif choice == "2":
                filename = input("저장할 파일명 (엔터시 자동 생성): ").strip()
                if not filename:
                    filename = None
                analyzer.export_complete_analysis(analysis, filename)
            
            elif choice == "3":
                print(f"\n📊 === 사용자별 데이터 통계 ===")
                user_counts = analysis.get('user_data_counts', {})
                total_users = len(user_counts)
                total_records = sum(user_counts.values())
                
                print(f"총 사용자 수: {total_users}명")
                print(f"총 데이터 수: {total_records}건")
                print(f"사용자당 평균 데이터: {total_records/total_users:.1f}건")
                print(f"\n사용자별 상세:")
                for user_id in sorted(user_counts.keys()):
                    print(f"  User {user_id}: {user_counts[user_id]}건")
            
            elif choice == "4":
                print("👋 분석기를 종료합니다.")
                break
            
            else:
                print("❌ 잘못된 선택입니다. 1-4 중에서 선택해주세요.")
    
    except Exception as e:
        print(f"❌ 오류 발생: {e}")


if __name__ == "__main__":
    main()