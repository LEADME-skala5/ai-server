#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MongoDB 연결 테스트 스크립트
간단하게 MongoDB 연결 상태만 확인하는 용도
"""

from pymongo import MongoClient
import sys

def test_mongodb_connection():
    """MongoDB 연결 테스트"""
    
    # 연결 정보
    host = '13.209.110.151'
    port = 27017
    username = 'root'
    password = 'root'
    database = 'skala'
    
    print("🎯 === MongoDB 연결 테스트 ===")
    print(f"호스트: {host}:{port}")
    print(f"사용자: {username}")
    print(f"데이터베이스: {database}")
    print()
    
    # 여러 연결 방법 시도
    connection_methods = [
        {
            "name": "기본 연결",
            "uri": f"mongodb://{username}:{password}@{host}:{port}/{database}"
        },
        {
            "name": "authSource=admin",
            "uri": f"mongodb://{username}:{password}@{host}:{port}/{database}?authSource=admin"
        },
        {
            "name": "authSource=admin + SCRAM-SHA-1",
            "uri": f"mongodb://{username}:{password}@{host}:{port}/{database}?authSource=admin&authMechanism=SCRAM-SHA-1"
        },
        {
            "name": "authSource=admin + SCRAM-SHA-256",
            "uri": f"mongodb://{username}:{password}@{host}:{port}/{database}?authSource=admin&authMechanism=SCRAM-SHA-256"
        }
    ]
    
    success_count = 0
    
    for i, method in enumerate(connection_methods, 1):
        print(f"🔄 방법 {i}: {method['name']}")
        
        try:
            # MongoDB 클라이언트 생성
            client = MongoClient(
                method['uri'],
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000
            )
            
            # 연결 테스트
            client.admin.command('ping')
            print("   ✅ Ping 성공")
            
            # 데이터베이스 목록 가져오기
            db_list = client.list_database_names()
            print(f"   📋 데이터베이스 목록: {db_list}")
            
            # 대상 데이터베이스 존재 확인
            if database in db_list:
                print(f"   ✅ '{database}' 데이터베이스 존재함")
                
                # 컬렉션 목록 가져오기
                db = client[database]
                collections = db.list_collection_names()
                print(f"   📂 컬렉션 목록: {collections}")
                
                # 샘플 컬렉션의 문서 수 확인
                if collections:
                    sample_collection = collections[0]
                    doc_count = db[sample_collection].count_documents({})
                    print(f"   📊 '{sample_collection}' 컬렉션 문서 수: {doc_count}개")
                    
                    # 샘플 문서 1개 조회
                    sample_doc = db[sample_collection].find_one()
                    if sample_doc:
                        print(f"   📄 샘플 문서 키: {list(sample_doc.keys())}")
                
            else:
                print(f"   ⚠️  '{database}' 데이터베이스가 존재하지 않습니다")
            
            client.close()
            print(f"   ✅ 방법 {i} 성공!")
            success_count += 1
            break  # 성공하면 다른 방법은 시도하지 않음
            
        except Exception as e:
            print(f"   ❌ 방법 {i} 실패: {e}")
        
        print()
    
    # 결과 요약
    print("=" * 50)
    if success_count > 0:
        print("🎉 MongoDB 연결 성공!")
        print("✅ 연결이 정상적으로 작동합니다.")
        return True
    else:
        print("💥 MongoDB 연결 실패!")
        print("❌ 모든 연결 방법이 실패했습니다.")
        print("\n🔧 해결 방법:")
        print("1. MongoDB 서버가 실행 중인지 확인")
        print("2. 네트워크 연결 상태 확인")
        print("3. 사용자명/비밀번호 확인")
        print("4. 방화벽 설정 확인")
        return False

def test_network_connectivity():
    """네트워크 연결성 테스트"""
    import socket
    
    host = '13.209.110.151'
    port = 27017
    
    print("🌐 네트워크 연결성 테스트")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f"✅ {host}:{port} 네트워크 연결 가능")
            return True
        else:
            print(f"❌ {host}:{port} 네트워크 연결 불가")
            return False
    except Exception as e:
        print(f"❌ 네트워크 테스트 실패: {e}")
        return False

if __name__ == "__main__":
    print("🚀 MongoDB 연결 진단 시작")
    print()
    
    # 1. 네트워크 연결성 테스트
    network_ok = test_network_connectivity()
    print()
    
    if network_ok:
        # 2. MongoDB 연결 테스트
        mongodb_ok = test_mongodb_connection()
        
        if mongodb_ok:
            print("\n🎯 결론: MongoDB 연결 준비 완료!")
            sys.exit(0)
        else:
            print("\n💥 결론: MongoDB 연결 설정 문제")
            sys.exit(1)
    else:
        print("\n💥 결론: 네트워크 연결 문제")
        print("서버가 다운되었거나 방화벽에 의해 차단되었을 수 있습니다.")
        sys.exit(1)