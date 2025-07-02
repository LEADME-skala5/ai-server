# D:\Github\ai-server\app\main.py

import os
import sys
from pathlib import Path
from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import asyncio
from dotenv import load_dotenv

# 현재 디렉토리 Python 경로에 추가
current_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(current_dir))

# .env 파일 명시적으로 로드
env_path = current_dir.parent / ".env"
load_dotenv(dotenv_path=env_path)
print(f"✅ .env 파일 로드: {env_path}")

# 환경변수 확인 (보안상 일부만 표시)
pinecone_key = os.getenv("PINECONE_API_KEY")
openai_key = os.getenv("OPENAI_API_KEY")
print(f"✅ ENV 체크 - PINECONE_API_KEY: {'설정됨' if pinecone_key else '없음'} ({len(pinecone_key) if pinecone_key else 0}자)")
print(f"✅ ENV 체크 - OPENAI_API_KEY: {'설정됨' if openai_key else '없음'} ({len(openai_key) if openai_key else 0}자)")

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# FastAPI 앱 생성
app = FastAPI(
    title="AI 성과관리 시스템 API",
    description="RAG 기반 주간 보고서 평가 시스템",
    version="1.0.0",
    openapi_url="/openapi.json",
    docs_url="/docs",
    redoc_url="/redoc"
)

# APIRouter 생성
base_router = APIRouter(prefix="/api/v1/ai")
weekly_report_router = APIRouter(prefix="/api/v1/ai/weekly-report", tags=["Weekly Report"])

# ===== 요청/응답 모델 =====
class EvaluationRequest(BaseModel):
    user_id: str

class BatchEvaluationRequest(BaseModel):
    user_ids: Optional[List[str]] = None  # None이면 모든 사용자

class EvaluationResponse(BaseModel):
    success: bool
    message: str
    data: Dict[str, Any]
    timestamp: str

class UsersResponse(BaseModel):
    success: bool
    total_users: int
    available_users: List[str]
    message: str

class HealthResponse(BaseModel):
    status: str
    pinecone_index: str
    namespace: str
    database: str
    model: str
    timestamp: str

# ===== 기존 에이전트 임포트 =====
def import_weekly_report_agent():
    """WeeklyReportEvaluationAgent 임포트"""
    try:
        # 경로 추가 (프로젝트 구조에 맞게 조정)
        project_root = current_dir.parent
        
        # 가능한 경로들 시도
        possible_paths = [
            project_root / "personal_quarter_reports" / "agents",
            project_root / "agents",
            current_dir / "agents",
            current_dir / "personal_quarter_reports" / "agents",
            current_dir  # 현재 디렉토리도 추가
        ]
        
        for path in possible_paths:
            if path.exists():
                sys.path.insert(0, str(path))
                logger.info(f"에이전트 경로 추가: {path}")
                break
        
        # 임포트 시도
        from weekly_report_reference import WeeklyReportEvaluationAgent
        logger.info("✅ WeeklyReportEvaluationAgent 임포트 성공")
        return WeeklyReportEvaluationAgent
        
    except ImportError as e:
        logger.error(f"❌ WeeklyReportEvaluationAgent 임포트 실패: {e}")
        # 현재 디렉토리에서 직접 임포트 시도
        try:
            import weekly_report_reference
            logger.info("✅ 직접 임포트 성공")
            return weekly_report_reference.WeeklyReportEvaluationAgent
        except ImportError as e2:
            logger.error(f"❌ 직접 임포트도 실패: {e2}")
            raise ImportError(
                f"WeeklyReportEvaluationAgent를 찾을 수 없습니다. "
                f"weekly_report_reference.py 파일이 올바른 위치에 있는지 확인해주세요. "
                f"원본 오류: {e}"
            )

# ===== 에이전트 관리 =====
# 전역 에이전트 인스턴스들
weekly_report_agent_instance = None

def create_weekly_report_agent():
    """WeeklyReportEvaluationAgent 생성"""
    try:
        WeeklyReportEvaluationAgent = import_weekly_report_agent()
        
        # API 키 확인 및 디버깅
        openai_key = os.getenv("OPENAI_API_KEY")
        pinecone_key = os.getenv("PINECONE_API_KEY")
        
        logger.info(f"🔑 OpenAI Key 길이: {len(openai_key) if openai_key else 0}")
        logger.info(f"🔑 Pinecone Key 길이: {len(pinecone_key) if pinecone_key else 0}")
        logger.info(f"🔑 Pinecone Key 앞부분: {pinecone_key[:10] + '...' if pinecone_key else 'None'}")
        
        if not openai_key:
            raise ValueError("OPENAI_API_KEY가 .env 파일에 설정되지 않았습니다.")
        if not pinecone_key:
            raise ValueError("PINECONE_API_KEY가 .env 파일에 설정되지 않았습니다.")
        
        # 에이전트 생성
        agent = WeeklyReportEvaluationAgent(
            openai_api_key=openai_key,
            pinecone_api_key=pinecone_key,
            model=os.getenv("OPENAI_MODEL", "gpt-4-turbo"),
            output_path=os.getenv("OUTPUT_PATH", "./output")
        )
        
        logger.info("✅ WeeklyReportEvaluationAgent 초기화 완료")
        return agent
        
    except Exception as e:
        logger.error(f"❌ 에이전트 초기화 실패: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"에이전트 초기화 실패: {str(e)}. API 키와 파일 경로를 확인해주세요."
        )

def get_weekly_report_agent():
    """WeeklyReportEvaluationAgent 인스턴스 가져오기 (싱글톤 패턴)"""
    global weekly_report_agent_instance
    if weekly_report_agent_instance is None:
        weekly_report_agent_instance = create_weekly_report_agent()
    return weekly_report_agent_instance


# ===== 기본 엔드포인트 =====

@app.get("/")
async def root():
    """루트 엔드포인트"""
    return {
        "message": "AI 성과관리 시스템 API", 
        "version": "1.0.0",
        "services": {
            "weekly_report_evaluation": "/api/v1/ai/weekly-report",
            "base_api": "/api/v1/ai",
            # 추후 다른 서비스들 추가 예정
            # "monthly_report_evaluation": "/api/v1/ai/monthly-report",
            # "quarterly_report_evaluation": "/api/v1/ai/quarterly-report"
        },
        "docs": "/docs",
        "status": "running"
    }

@app.get("/health")
async def global_health():
    """전체 시스템 상태 확인"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": ["weekly_report_evaluation"],
        "env_loaded": bool(os.getenv("PINECONE_API_KEY"))
    }

# ===== 기본 API 라우터 엔드포인트 =====

@base_router.get("/hello")
async def read_root():
    """기본 테스트 엔드포인트"""
    return {
        "message": "Hello World from AI API",
        "timestamp": datetime.now().isoformat(),
        "status": "running"
    }

@base_router.get("/status")
async def api_status():
    """API 상태 확인"""
    return {
        "api_status": "healthy",
        "version": "1.0.0",
        "available_services": ["weekly_report"],
        "timestamp": datetime.now().isoformat()
    }

# ===== 주간 보고서 API 라우터 엔드포인트 =====

@weekly_report_router.get("/health", response_model=HealthResponse)
async def weekly_report_health_check():
    """주간 보고서 평가 시스템 상태 확인"""
    try:
        agent = get_weekly_report_agent()
        return HealthResponse(
            status="healthy",
            pinecone_index=agent.pinecone_index_name,
            namespace=agent.namespace,
            database=f"{agent.db_config['host']}:{agent.db_config['port']}/{agent.db_config['database']}",
            model=agent.model,
            timestamp=datetime.now().isoformat()
        )
    except Exception as e:
        logger.error(f"Health check 실패: {e}")
        raise HTTPException(status_code=500, detail=f"시스템 상태 확인 실패: {str(e)}")

@weekly_report_router.get("/users", response_model=UsersResponse)
async def get_weekly_report_users():
    """주간 보고서 평가 가능한 사용자 목록 조회"""
    try:
        logger.info("주간 보고서 사용자 목록 조회 시작")
        
        agent = get_weekly_report_agent()
        available_users = agent.get_available_user_ids()
        
        return UsersResponse(
            success=True,
            total_users=len(available_users),
            available_users=available_users,
            message=f"주간 보고서 평가 가능한 사용자 총 {len(available_users)}명 발견"
        )
        
    except Exception as e:
        logger.error(f"사용자 목록 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=f"사용자 목록 조회 실패: {str(e)}")

@weekly_report_router.post("/evaluate", response_model=EvaluationResponse)
async def evaluate_weekly_report(request: EvaluationRequest):
    """주간 보고서 RAG 평가 실행 - 핵심 기능"""
    try:
        logger.info(f"사용자 {request.user_id} 주간 보고서 RAG 평가 시작")

        agent = get_weekly_report_agent()

        # 사용자 존재 여부 확인
        available_users = agent.get_available_user_ids()
        if request.user_id not in available_users:
            raise HTTPException(
                status_code=404, 
                detail=f"사용자 ID '{request.user_id}'를 찾을 수 없습니다. 사용 가능한 사용자: {available_users}"
            )

        # 실제 평가 실행 (블로킹 작업을 별도 스레드에서 실행)
        result = await asyncio.get_event_loop().run_in_executor(
            None, 
            agent.execute_single_evaluation,
            request.user_id
        )

        # 결과 확인
        if "error" in result:
            logger.error(f"사용자 {request.user_id} 평가 실패: {result['error']}")
            return EvaluationResponse(
                success=False,
                message=f"주간 보고서 평가 실패: {result['error']}",
                data=result,
                timestamp=datetime.now().isoformat()
            )
        else:
            logger.info(f"사용자 {request.user_id} 주간 보고서 RAG 평가 완료")
            return EvaluationResponse(
                success=True,
                message=f"사용자 {request.user_id} 주간 보고서 RAG 평가 완료",
                data=result,
                timestamp=datetime.now().isoformat()
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"평가 처리 중 예외 발생: {e}")
        raise HTTPException(status_code=500, detail=f"평가 처리 중 오류: {str(e)}")

@weekly_report_router.post("/batch-evaluate")
async def batch_evaluate_weekly_report(request: BatchEvaluationRequest):
    """주간 보고서 배치 평가 실행"""
    try:
        logger.info("주간 보고서 배치 평가 시작")
        
        agent = get_weekly_report_agent()
        
        # 배치 평가 실행 (블로킹 작업을 별도 스레드에서 실행)
        result = await asyncio.get_event_loop().run_in_executor(
            None, 
            agent.execute_batch_evaluation,
            request.user_ids
        )
        
        return {
            "success": True,
            "message": "주간 보고서 배치 평가 완료",
            "data": result,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"배치 평가 실패: {e}")
        raise HTTPException(status_code=500, detail=f"배치 평가 실패: {str(e)}")


# ===== 라우터 등록 =====
app.include_router(base_router)
app.include_router(weekly_report_router)

# ===== 추후 다른 서비스들 추가할 공간 =====

# monthly_report_router = APIRouter(prefix="/api/v1/monthly-report", tags=["Monthly Report"])
# quarterly_report_router = APIRouter(prefix="/api/v1/quarterly-report", tags=["Quarterly Report"])
# app.include_router(monthly_report_router)
# app.include_router(quarterly_report_router)

# 메인 실행부
if __name__ == "__main__":
    import uvicorn
    
    print("🚀 AI 성과관리 시스템 API 서버 시작")
    print("📝 API 문서: http://localhost:8000/docs")
    print("🎯 주간 보고서 평가: http://localhost:8000/api/v1/ai/weekly-report/evaluate")
    print("👥 사용자 목록: http://localhost:8000/api/v1/ai/weekly-report/users")
    print("🔧 기본 API: http://localhost:8000/api/v1/ai/hello")
    print("💊 Health Check: http://localhost:8000/health")
    
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)