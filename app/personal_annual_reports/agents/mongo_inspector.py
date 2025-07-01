#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
weekly_evaluation_results 컬렉션 간단 분석기 (딕셔너리 구조용)
user_id 위치만 빠르게 찾는 도구
"""

from pymongo import MongoClient
import json

def analyze_weekly_collection_simple():
    """weekly_evaluation_results 컬렉션에서 user_id만 찾습니다."""
    
    connection_string = "mongodb://root:root@13.209.110.151:27017/skala?authSource=admin"
    
    print("🔍 === weekly_evaluation_results 분석 ===")
    
    try:
        client = MongoClient(connection_string, serverSelectionTimeoutMS=10000)
        db = client['skala']
        collection = db['weekly_evaluation_results']
        
        # 1. 기본 정보
        doc_count = collection.count_documents({})
        print(f"📊 문서 수: {doc_count}개")
        
        if doc_count == 0:
            print("❌ 컬렉션이 비어있습니다.")
            return
        
        # 2. 첫 번째 문서의 최상위 구조만 확인
        first_doc = collection.find_one()
        print(f"\n📋 최상위 필드:")
        for key, value in first_doc.items():
            value_type = type(value).__name__
            if isinstance(value, dict):
                print(f"   - {key}: dict({len(value)} keys)")
            elif isinstance(value, list):
                print(f"   - {key}: list({len(value)} items)")
            else:
                print(f"   - {key}: {value_type}")
        
        # 3. users 딕셔너리 구조 확인
        print(f"\n🎯 users 딕셔너리 분석:")
        
        if 'users' in first_doc and isinstance(first_doc['users'], dict):
            users_dict = first_doc['users']
            user_keys = list(users_dict.keys())
            
            print(f"✅ users 딕셔너리 발견:")
            print(f"   📊 사용자 수: {len(user_keys)}명")
            print(f"   🔑 사용자 키 (처음 5개): {user_keys[:5]}")
            
            # 첫 번째 사용자 구조 확인
            if user_keys:
                first_user_key = user_keys[0]
                first_user_data = users_dict[first_user_key]
                
                print(f"\n📄 사용자 '{first_user_key}' 구조:")
                if isinstance(first_user_data, dict):
                    for key, value in first_user_data.items():
                        value_type = type(value).__name__
                        if isinstance(value, dict):
                            print(f"   - {key}: dict({len(value)} keys)")
                        elif isinstance(value, list):
                            print(f"   - {key}: list({len(value)} items)")
                        else:
                            preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                            print(f"   - {key}: {value_type} = {preview}")
                
                # user_id 확인
                if isinstance(first_user_data, dict) and 'user_id' in first_user_data:
                    print(f"\n✅ user_id 발견: users.{first_user_key}.user_id = {first_user_data['user_id']}")
                    suggest_dict_extraction(user_keys)
                else:
                    print(f"\n❌ user_id 필드가 없습니다.")
        else:
            print(f"❌ users 딕셔너리를 찾을 수 없습니다.")
        
        client.close()
        
    except Exception as e:
        print(f"❌ 분석 실패: {e}")

def suggest_dict_extraction(user_keys):
    """딕셔너리 구조용 추출 방법 제안"""
    print(f"\n💡 user_id 추출 방법 (딕셔너리 구조):")
    
    print(f"   🔧 방법 1 - 딕셔너리 키 사용:")
    print(f"      user_ids = list(collection.find_one()['users'].keys())")
    
    print(f"   🔧 방법 2 - user_id 필드값 사용:")
    print(f"      pipeline = [")
    print(f"          {{\"$project\": {{\"user_values\": {{\"$objectToArray\": \"$users\"}}}}}},")
    print(f"          {{\"$unwind\": \"$user_values\"}},")
    print(f"          {{\"$group\": {{\"_id\": \"$user_values.v.user_id\"}}}}")
    print(f"      ]")
    
    print(f"   🔧 방법 3 - 간단한 순회:")
    print(f"      for doc in collection.find():")
    print(f"          for user_id, user_data in doc['users'].items():")
    print(f"              print(user_id)  # 또는 user_data['user_id']")
    
    print(f"\n📝 샘플 사용자 키: {user_keys[:10]}")

if __name__ == "__main__":
    analyze_weekly_collection_simple()