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
score_router = APIRouter(prefix="/api/v1/ai/score", tags=["Score Evaluation"])
ranking_router = APIRouter(prefix="/api/v1/ai/report", tags=["Ranking Report"])

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
    
class ScoreEvaluationRequest(BaseModel):
    user_id: str
    year: int = 2024
    quarter: int = 4
    include_details: Optional[bool] = True

class ScoreEvaluationResponse(BaseModel):
    success: bool
    message: str
    data: Dict[str, Any]
    timestamp: str
    
class RankingEvaluationRequest(BaseModel):
    user_id: str
    year: int = 2024
    quarter: int = 4
    include_details: Optional[bool] = True

class RankingEvaluationResponse(BaseModel):
    success: bool
    message: str
    data: Dict[str, Any]
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
            
def import_score_modules():
    """Score 관련 모듈들 임포트"""
    try:
        # 경로 추가
        agents_path = current_dir / "personal_quarter_reports" / "agents"
        if agents_path.exists():
            sys.path.insert(0, str(agents_path))
            logger.info(f"Score 에이전트 경로 추가: {agents_path}")
        
        # 각 모듈 임포트
        modules = {}
        
        # Weekly 평가 모듈
        try:
            import weekly_evaluations
            modules['weekly'] = weekly_evaluations
            logger.info("✅ weekly_evaluations 임포트 성공")
        except ImportError as e:
            logger.warning(f"⚠️ weekly_evaluations 임포트 실패: {e}")
            modules['weekly'] = None
        
        # Peer 평가 모듈
        try:
            import peer_personal_agent_v2
            modules['peer'] = peer_personal_agent_v2
            logger.info("✅ peer_personal_agent_v2 임포트 성공")
        except ImportError as e:
            logger.warning(f"⚠️ peer_personal_agent_v2 임포트 실패: {e}")
            modules['peer'] = None
        
        # Qualitative 평가 모듈
        try:
            import personal_qualitative_evaluations
            modules['qualitative'] = personal_qualitative_evaluations
            logger.info("✅ personal_qualitative_evaluations 임포트 성공")
        except ImportError as e:
            logger.warning(f"⚠️ personal_qualitative_evaluations 임포트 실패: {e}")
            modules['qualitative'] = None
        
        # Final Score 계산 모듈
        try:
            import calculate_final_score
            modules['final'] = calculate_final_score
            logger.info("✅ calculate_final_score 임포트 성공")
        except ImportError as e:
            logger.warning(f"⚠️ calculate_final_score 임포트 실패: {e}")
            modules['final'] = None
        
        return modules
        
    except Exception as e:
        logger.error(f"❌ Score 모듈 임포트 실패: {e}")
        return {}

def import_ranking_modules():
    """Ranking 관련 모듈들 임포트 - 딕셔너리 반환"""
    try:
        # 경로 추가
        agents_path = current_dir / "personal_quarter_reports" / "agents"
        if agents_path.exists():
            sys.path.insert(0, str(agents_path))
            logger.info(f"Ranking 에이전트 경로 추가: {agents_path}")
        else:
            logger.error(f"❌ 에이전트 경로가 존재하지 않음: {agents_path}")
        
        modules = {}
        
        # Ranking 평가 모듈 임포트
        try:
            import ranking_evaluation_agent
            modules['ranking'] = ranking_evaluation_agent
            logger.info("✅ ranking_evaluation_agent 임포트 성공")
        except ImportError as e:
            logger.error(f"❌ ranking_evaluation_agent 임포트 실패: {e}")
            modules['ranking'] = None
        
        # 종합 리포트 생성 모듈 임포트
        try:
            import generate_quarterly_report
            modules['report'] = generate_quarterly_report
            logger.info("✅ generate_quarterly_report 임포트 성공")
        except ImportError as e:
            logger.error(f"❌ generate_quarterly_report 임포트 실패: {e}")
            modules['report'] = None
        except Exception as e:
            logger.error(f"❌ generate_quarterly_report 예외 발생: {e}")
            modules['report'] = None
        
        logger.info(f"🔍 최종 모듈 상태: {list(modules.keys())}")
        return modules
        
    except Exception as e:
        logger.error(f"❌ Ranking 모듈 임포트 전체 실패: {e}")
        return {}
    
# 전역 모듈 저장소 (싱글톤 패턴)
weekly_report_agent_instance = None
score_modules = None
ranking_modules = None

def get_weekly_report_agent():
    """WeeklyReportEvaluationAgent 인스턴스 가져오기 (싱글톤 패턴)"""
    global weekly_report_agent_instance
    if weekly_report_agent_instance is None:
        weekly_report_agent_instance = create_weekly_report_agent()
    return weekly_report_agent_instance

def get_score_modules():
    """Score 모듈들 가져오기 (싱글톤 패턴)"""
    global score_modules
    if score_modules is None:
        score_modules = import_score_modules()
    return score_modules

def get_ranking_modules():
    """Ranking 모듈들 가져오기 (싱글톤 패턴)"""
    global ranking_modules
    if ranking_modules is None:
        ranking_modules = import_ranking_modules()
    return ranking_modules


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


# ===== 기본 엔드포인트 =====

@app.get("/")
async def root():
    """루트 엔드포인트"""
    return {
        "message": "AI 성과관리 시스템 API", 
        "version": "1.0.0",
        "services": {
            "weekly_report_evaluation": "/api/v1/ai/weekly-report",
            "score_evaluation": "/api/v1/ai/score",
            "ranking_report": "/api/v1/ai/report",
            "base_api": "/api/v1/ai",
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
        "services": ["weekly_report_evaluation", "score_evaluation", "ranking_report"],
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
        "available_services": ["weekly_report", "score"],  # score 추가
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
    
# ===== Score 평가 API 라우터 엔드포인트 =====

@score_router.get("/health")
async def score_health_check():
    """Score 평가 시스템 상태 확인"""
    try:
        modules = get_score_modules()
        
        # 모듈 상태 확인
        module_status = {
            "weekly_evaluations": modules.get('weekly') is not None,
            "peer_evaluations": modules.get('peer') is not None,
            "qualitative_evaluations": modules.get('qualitative') is not None,
            "final_score_calculator": modules.get('final') is not None
        }
        
        # DB 연결 테스트 (weekly 모듈을 통해)
        if modules.get('weekly'):
            try:
                conn = modules['weekly'].get_connection()
                conn.close()
                db_status = "connected"
            except:
                db_status = "disconnected"
        else:
            db_status = "unknown"
        
        return {
            "status": "healthy",
            "modules": module_status,
            "database": db_status,
            "score_types": ["weekly_score", "peer_score", "qualitative_score", "final_score"],
            "calculation_method": "weighted_average_40_30_30",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Score Health check 실패: {e}")
        raise HTTPException(status_code=500, detail=f"Score 시스템 상태 확인 실패: {str(e)}")

@score_router.get("/users", response_model=UsersResponse)
async def get_score_users():
    """Score 평가 가능한 사용자 목록 조회"""
    try:
        logger.info("Score 사용자 목록 조회 시작")
        
        modules = get_score_modules()
        
        # weekly 모듈을 사용해서 사용자 목록 조회
        if modules.get('weekly'):
            try:
                conn = modules['weekly'].get_connection()
                cursor = conn.cursor()
                
                # user_quarter_scores 테이블에서 사용자 목록 조회
                cursor.execute("SELECT DISTINCT user_id FROM user_quarter_scores ORDER BY user_id")
                results = cursor.fetchall()
                conn.close()
                
                # 결과 형태에 따라 처리
                if isinstance(results[0], dict):
                    available_users = [str(row['user_id']) for row in results]
                else:
                    available_users = [str(row[0]) for row in results]
                    
            except Exception as db_error:
                logger.warning(f"DB 조회 실패: {db_error}, fallback 사용")
                # fallback으로 하드코딩된 사용자 목록
                available_users = ["82", "83", "84", "85", "86"]
        else:
            # fallback으로 하드코딩된 사용자 목록
            available_users = ["82", "83", "84", "85", "86"]
        
        return UsersResponse(
            success=True,
            total_users=len(available_users),
            available_users=available_users,
            message=f"Score 평가 가능한 사용자 총 {len(available_users)}명 발견"
        )
        
    except Exception as e:
        logger.error(f"Score 사용자 목록 조회 실패: {e}")
        # 완전 fallback
        return UsersResponse(
            success=True,
            total_users=5,
            available_users=["82", "83", "84", "85", "86"],
            message="DB 연결 실패로 기본 사용자 목록 반환"
        )

def get_qualitative_score_for_user(qualitative_module, user_id: int, year: int, quarter: int) -> float:
    """정성평가 점수 계산 헬퍼 함수"""
    try:
        # pandas와 sqlalchemy가 있는지 확인
        import pandas as pd
        from sqlalchemy import create_engine
        
        # DB 연결 (qualitative 모듈의 설정 사용)
        engine = qualitative_module.engine
        
        # 해당 사용자 데이터 조회
        df = pd.read_sql(
            f"SELECT * FROM user_qualitative_evaluations WHERE user_id = {user_id} AND evaluation_year = {year} AND evaluation_quarter = {quarter}",
            engine
        )
        
        if df.empty:
            return 0.0
        
        # calculate_total_score 함수 사용
        row = df.iloc[0]
        score = qualitative_module.calculate_total_score(row)
        
        return score if score else 0.0
        
    except Exception as e:
        logger.error(f"정성평가 점수 계산 실패: {e}")
        return 0.0

@score_router.post("/evaluate", response_model=ScoreEvaluationResponse)
async def evaluate_score(request: ScoreEvaluationRequest):
    """종합 Score 평가 실행 - 핵심 기능 (기존 모듈 재사용)"""
    try:
        start_time = datetime.now()
        logger.info(f"🏆 사용자 {request.user_id} 종합 Score 평가 시작 ({request.year}년 {request.quarter}분기)")
        
        modules = get_score_modules()
        user_id = int(request.user_id)
        year = request.year
        quarter = request.quarter
        
        # 개별 스코어 저장소
        scores = {}
        score_details = {}
        errors = {}
        
        # 1. Weekly Score 계산 (기존 weekly_evaluations.py 사용)
        try:
            logger.info("📊 Weekly Score 계산 시작")
            
            if modules.get('weekly'):
                # 기존 함수들 직접 호출 (매개변수 순서 확인)
                avg_grade = await asyncio.get_event_loop().run_in_executor(
                    None, 
                    lambda: modules['weekly'].get_average_grade(user_id, year, quarter)
                )
                workload_score = await asyncio.get_event_loop().run_in_executor(
                    None, 
                    lambda: modules['weekly'].get_weighted_workload_score(user_id, year, quarter)
                )
                
                # 0이 아닌 경우에만 최종 계산
                if avg_grade > 0 or workload_score > 0:
                    weekly_score = modules['weekly'].calculate_final_score(avg_grade, workload_score)
                else:
                    weekly_score = 0.0
                    logger.warning(f"Weekly Score 데이터 없음: avg_grade={avg_grade}, workload_score={workload_score}")
                
                scores['weekly_score'] = weekly_score
                if request.include_details:
                    score_details['weekly_score'] = {
                        "average_grade": avg_grade,
                        "workload_score": workload_score,
                        "final_weekly_score": weekly_score,
                        "source": "weekly_evaluations.py"
                    }
                logger.info(f"✅ Weekly Score 계산 완료: {weekly_score}")
            else:
                scores['weekly_score'] = 0.0
                errors['weekly_score'] = "weekly_evaluations 모듈을 사용할 수 없습니다."
            
        except Exception as e:
            logger.error(f"❌ Weekly Score 계산 실패: {e}")
            errors['weekly_score'] = str(e)
            scores['weekly_score'] = 0.0
        
        # 2. Peer Score 계산 (기존 peer_personal_agent_v2.py 사용)
        try:
            logger.info("👥 Peer Score 계산 시작")
            
            if modules.get('peer'):
                # 기존 PeerEvaluationOrchestrator 사용 (매개변수 개수 맞춤)
                openai_key = os.getenv("OPENAI_API_KEY")
                orchestrator = modules['peer'].PeerEvaluationOrchestrator(openai_key)
                
                # process_peer_evaluation 메서드 사용 (4개 매개변수)
                peer_result = await asyncio.get_event_loop().run_in_executor(
                    None, 
                    lambda: orchestrator.process_peer_evaluation(user_id, year, quarter)
                )
                
                if peer_result["success"]:
                    peer_score = peer_result["data"]["peer_evaluation_score"]
                    scores['peer_score'] = peer_score
                    if request.include_details:
                        score_details['peer_score'] = {
                            "final_peer_score": peer_score,
                            "feedback": peer_result["data"]["feedback"],
                            "source": "peer_personal_agent_v2.py"
                        }
                    logger.info(f"✅ Peer Score 계산 완료: {peer_score}")
                else:
                    scores['peer_score'] = 0.0
                    errors['peer_score'] = peer_result["message"]
            else:
                scores['peer_score'] = 0.0
                errors['peer_score'] = "peer_personal_agent_v2 모듈을 사용할 수 없습니다."
                
        except Exception as e:
            logger.error(f"❌ Peer Score 계산 실패: {e}")
            errors['peer_score'] = str(e)
            scores['peer_score'] = 0.0
        
        # 3. Qualitative Score 계산 (기존 함수 직접 사용)
        try:
            logger.info("📝 Qualitative Score 계산 시작")
            
            if modules.get('qualitative'):
                # calculate_total_score 함수를 직접 사용하기 위해 데이터 조회
                qualitative_score = await asyncio.get_event_loop().run_in_executor(
                    None, 
                    lambda: get_qualitative_score_for_user(modules['qualitative'], user_id, year, quarter)
                )
                
                scores['qualitative_score'] = qualitative_score
                if request.include_details:
                    score_details['qualitative_score'] = {
                        "final_qualitative_score": qualitative_score,
                        "source": "personal_qualitative_evaluations.py"
                    }
                logger.info(f"✅ Qualitative Score 계산 완료: {qualitative_score}")
            else:
                scores['qualitative_score'] = 0.0
                errors['qualitative_score'] = "personal_qualitative_evaluations 모듈을 사용할 수 없습니다."
            
        except Exception as e:
            logger.error(f"❌ Qualitative Score 계산 실패: {e}")
            errors['qualitative_score'] = str(e)
            scores['qualitative_score'] = 0.0
        
        # 4. Final Score 계산 (직접 계산)
        try:
            # 가중 평균 계산 (40% : 30% : 30%)
            final_score = round(
                0.4 * scores['weekly_score'] + 
                0.3 * scores['qualitative_score'] + 
                0.3 * scores['peer_score'], 
                2
            )
            calculation_method = "weighted_average_40_30_30"
            
            # 등급 계산
            if final_score >= 4.5:
                grade = "A+"
            elif final_score >= 4.0:
                grade = "A"
            elif final_score >= 3.5:
                grade = "B+"
            elif final_score >= 3.0:
                grade = "B"
            elif final_score >= 2.5:
                grade = "C+"
            elif final_score >= 2.0:
                grade = "C"
            else:
                grade = "D"
                
        except Exception as e:
            logger.error(f"❌ Final Score 계산 실패: {e}")
            final_score = 0.0
            grade = "F"
            calculation_method = "calculation_failed"
        
        # 5. 최종 결과 구성
        processing_time = (datetime.now() - start_time).total_seconds()
        
        result_data = {
            "user_id": request.user_id,
            "year": year,
            "quarter": quarter,
            "final_score": final_score,
            "grade": grade,
            "individual_scores": {
                "weekly_score": scores['weekly_score'],
                "peer_score": scores['peer_score'],
                "qualitative_score": scores['qualitative_score']
            },
            "calculation_method": calculation_method,
            "weights": {
                "weekly": "40%",
                "peer": "30%",
                "qualitative": "30%"
            },
            "processing_time_seconds": round(processing_time, 2),
            "evaluation_timestamp": datetime.now().isoformat(),
            "source_modules": {
                "weekly": "weekly_evaluations.py",
                "peer": "peer_personal_agent_v2.py", 
                "qualitative": "personal_qualitative_evaluations.py",
                "final": "직접계산"
            }
        }
        
        # 상세 정보 포함
        if request.include_details:
            result_data["score_details"] = score_details
        
        # 에러 정보 포함 (있는 경우)
        if errors:
            result_data["errors"] = errors
            result_data["warnings"] = "일부 스코어 계산에서 오류가 발생했습니다."
        
        # 성공 여부 결정 (최소 1개 스코어라도 있으면 성공)
        successful_scores = len([s for s in scores.values() if s > 0])
        success = successful_scores >= 1
        
        if success:
            logger.info(f"🎉 사용자 {request.user_id} 종합 Score 평가 완료 - Final Score: {final_score} ({grade})")
            return ScoreEvaluationResponse(
                success=True,
                message=f"사용자 {request.user_id} 종합 Score 평가 완료 (Final Score: {final_score}, Grade: {grade}, 처리시간: {processing_time:.2f}초)",
                data=result_data,
                timestamp=datetime.now().isoformat()
            )
        else:
            logger.error(f"❌ 사용자 {request.user_id} Score 평가 실패 - 모든 스코어 계산 실패")
            return ScoreEvaluationResponse(
                success=False,
                message=f"사용자 {request.user_id} Score 평가 실패 - 모든 개별 스코어 계산에서 오류 발생",
                data=result_data,
                timestamp=datetime.now().isoformat()
            )
        
    except HTTPException:
        raise
    except Exception as e:
        processing_time = (datetime.now() - start_time).total_seconds() if 'start_time' in locals() else 0
        logger.error(f"💥 Score 평가 처리 중 예외 발생 (소요시간: {processing_time:.2f}초): {e}")
        raise HTTPException(status_code=500, detail=f"Score 평가 처리 중 오류: {str(e)}")

# ===== Ranking Report API 라우터 엔드포인트 =====

@ranking_router.get("/health")
async def ranking_health_check():
    """Ranking Report 시스템 상태 확인"""
    try:
        ranking_modules = get_ranking_modules()
        
        if ranking_modules is None or not ranking_modules.get('ranking'):
            return {
                "status": "unhealthy",
                "message": "ranking_evaluation_agent 모듈을 사용할 수 없습니다",
                "timestamp": datetime.now().isoformat()
            }
        
        # RankingEvaluationSystem 클래스 확인
        ranking_system = ranking_modules['ranking'].RankingEvaluationSystem()
        
        # DB 연결 테스트
        try:
            conn = ranking_system.get_db_connection()
            conn.close()
            db_status = "connected"
        except:
            db_status = "disconnected"
        
        # MongoDB 연결 테스트
        mongodb_status = "disconnected"
        try:
            if ranking_system.mongodb_manager.connect():
                mongodb_status = "connected"
                ranking_system.mongodb_manager.close()
        except:
            pass
        
        # 종합 리포트 생성기 상태 확인
        report_generator_status = "unavailable"
        if ranking_modules.get('report'):
            report_generator_status = "available"
        
        return {
            "status": "healthy",
            "database": {
                "mariadb": db_status,
                "mongodb": mongodb_status
            },
            "modules": {
                "ranking_evaluation": ranking_modules.get('ranking') is not None,
                "comprehensive_report": ranking_modules.get('report') is not None
            },
            "features": [
                "user_ranking_calculation", 
                "team_ranking_calculation", 
                "ranking_report_generation",
                "comprehensive_quarter_report",
                "mongodb_storage"
            ],
            "ranking_types": ["same_job_rank", "organization_rank"],
            "source_modules": ["ranking_evaluation_agent.py", "generate_quarterly_report.py"],
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Ranking Health check 실패: {e}")
        raise HTTPException(status_code=500, detail=f"Ranking 시스템 상태 확인 실패: {str(e)}")

@ranking_router.get("/users", response_model=UsersResponse)
async def get_ranking_users():
    """Ranking Report 가능한 사용자 목록 조회"""
    try:
        logger.info("Ranking 사용자 목록 조회 시작")
        
        ranking_modules = get_ranking_modules()
        logger.info(f"🔍 랭킹 모듈 타입: {type(ranking_modules)}")
        
        if ranking_modules is None:
            logger.error("❌ ranking_modules가 None입니다")
            raise HTTPException(status_code=500, detail="ranking_modules가 None입니다")
        
        if not isinstance(ranking_modules, dict):
            logger.error(f"❌ ranking_modules가 딕셔너리가 아닙니다: {type(ranking_modules)}")
            raise HTTPException(status_code=500, detail="ranking_modules 타입 오류")
        
        if not ranking_modules.get('ranking'):
            logger.error("❌ ranking 모듈이 없습니다")
            raise HTTPException(status_code=500, detail="ranking_evaluation_agent 모듈을 사용할 수 없습니다")
        
        # RankingEvaluationSystem 인스턴스 생성
        ranking_system = ranking_modules['ranking'].RankingEvaluationSystem()
        
        # 기본적으로 2024년 4분기 기준으로 사용자 목록 조회
        available_users = ranking_system.get_all_users_with_ranking(2024, 4)
        
        # str 형태로 변환
        available_users = [str(user_id) for user_id in available_users]
        
        return UsersResponse(
            success=True,
            total_users=len(available_users),
            available_users=available_users,
            message=f"Ranking Report 가능한 사용자 총 {len(available_users)}명 발견 (2024년 4분기 기준)"
        )
        
    except Exception as e:
        logger.error(f"Ranking 사용자 목록 조회 실패: {e}")
        # fallback으로 기본 사용자 목록
        return UsersResponse(
            success=True,
            total_users=5,
            available_users=["82", "83", "84", "85", "86"],
            message="DB 연결 실패로 기본 사용자 목록 반환"
        )

@ranking_router.post("/evaluate", response_model=RankingEvaluationResponse)
async def evaluate_ranking_report(request: RankingEvaluationRequest):
    """사용자 랭킹 리포트 + 종합 성과 리포트 생성 - 핵심 기능"""
    try:
        start_time = datetime.now()
        logger.info(f"🏆 사용자 {request.user_id} 종합 리포트 생성 시작 ({request.year}년 {request.quarter}분기)")
        
        ranking_modules = get_ranking_modules()
        
        if ranking_modules is None or not ranking_modules.get('ranking'):
            raise HTTPException(status_code=500, detail="ranking_evaluation_agent 모듈을 사용할 수 없습니다")
        
        user_id = int(request.user_id)
        year = request.year
        quarter = request.quarter
        
        # 1. 랭킹 평가 실행 (기존 기능)
        ranking_system = ranking_modules['ranking'].RankingEvaluationSystem()
        
        ranking_result = await asyncio.get_event_loop().run_in_executor(
            None, 
            lambda: ranking_system.process_user_ranking_evaluation(
                user_id, year, quarter, save_to_mongodb=True
            )
        )
        
        # 2. 종합 리포트 생성 (새로운 기능)
        comprehensive_report = None
        if ranking_modules.get('report'):
            try:
                logger.info(f"📊 사용자 {user_id} 종합 성과 리포트 생성 중...")
                
                # ComprehensiveReportGenerator 인스턴스 생성
                report_generator = ranking_modules['report'].ComprehensiveReportGenerator()
                
                # MongoDB 연결
                if report_generator.connect():
                    # 종합 리포트 생성
                    comprehensive_report = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: report_generator.generate_comprehensive_report(user_id, year, quarter)
                    )
                    
                    # reports 컬렉션에 저장
                    save_success = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: report_generator.save_report_to_quarter_collection(comprehensive_report)
                    )
                    
                    if save_success:
                        logger.info(f"✅ 사용자 {user_id} 종합 리포트 MongoDB 저장 완료")
                    else:
                        logger.warning(f"⚠️ 사용자 {user_id} 종합 리포트 MongoDB 저장 실패")
                    
                    # 연결 종료
                    report_generator.close()
                else:
                    logger.error("❌ MongoDB 연결 실패로 종합 리포트 생성 불가")
                    
            except Exception as e:
                logger.error(f"❌ 종합 리포트 생성 실패: {e}")
                comprehensive_report = None
        else:
            logger.warning("⚠️ generate_quarterly_report 모듈을 사용할 수 없어 종합 리포트 생성 생략")
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # 3. 결과 구성
        if ranking_result["success"]:
            ranking_data = ranking_result["data"]
            
            # 응답 데이터 구성
            response_data = {
                "user_id": request.user_id,
                "year": year,
                "quarter": quarter,
                "ranking_info": ranking_data["ranking_info"],
                "scores": ranking_data["scores"],
                "result_text": ranking_data["result_text"],
                "processing_time_seconds": round(processing_time, 2),
                "evaluation_timestamp": ranking_data["processed_at"],
                "mongodb_saved": True,
                "source_modules": ["ranking_evaluation_agent.py", "generate_quarterly_report.py"]
            }
            
            # 종합 리포트 데이터 추가
            if comprehensive_report:
                response_data["comprehensive_report"] = comprehensive_report
                response_data["comprehensive_report_generated"] = True
                logger.info(f"✅ 종합 리포트 포함하여 응답 구성 완료")
            else:
                response_data["comprehensive_report_generated"] = False
                response_data["comprehensive_report_error"] = "종합 리포트 생성 실패 또는 모듈 없음"
            
            # 상세 정보 포함
            if request.include_details:
                response_data["details"] = {
                    "job_name": ranking_data["ranking_info"]["job_name"],
                    "job_years": ranking_data["ranking_info"]["job_years"],
                    "organization_id": ranking_data["ranking_info"]["organization_id"],
                    "calculation_method": "direct_database_ranking",
                    "ranking_source": "user_quarter_scores table",
                    "comprehensive_report_source": "multiple_collections_aggregated"
                }
            
            logger.info(f"🎉 사용자 {request.user_id} 종합 리포트 생성 완료 - 처리시간: {processing_time:.2f}초")
            
            return RankingEvaluationResponse(
                success=True,
                message=f"사용자 {request.user_id} 종합 리포트 생성 완료 (랭킹 + 성과 리포트, 처리시간: {processing_time:.2f}초)",
                data=response_data,
                timestamp=datetime.now().isoformat()
            )
        else:
            logger.error(f"❌ 사용자 {request.user_id} 랭킹 리포트 생성 실패")
            
            return RankingEvaluationResponse(
                success=False,
                message=f"사용자 {request.user_id} 리포트 생성 실패: {ranking_result.get('message', '알 수 없는 오류')}",
                data={
                    "user_id": request.user_id,
                    "year": year,
                    "quarter": quarter,
                    "error": ranking_result.get('message', '알 수 없는 오류'),
                    "processing_time_seconds": round(processing_time, 2),
                    "comprehensive_report_generated": False
                },
                timestamp=datetime.now().isoformat()
            )
        
    except HTTPException:
        raise
    except Exception as e:
        processing_time = (datetime.now() - start_time).total_seconds() if 'start_time' in locals() else 0
        logger.error(f"💥 종합 리포트 생성 중 예외 발생 (소요시간: {processing_time:.2f}초): {e}")
        raise HTTPException(status_code=500, detail=f"종합 리포트 생성 중 오류: {str(e)}")

@ranking_router.get("/stats")
async def get_ranking_stats():
    """Ranking Report 통계 조회"""
    try:
        ranking_modules = get_ranking_modules()
        
        if ranking_modules is None or not ranking_modules.get('ranking'):
            return {
                "success": False,
                "message": "ranking_evaluation_agent 모듈을 사용할 수 없습니다",
                "timestamp": datetime.now().isoformat()
            }
        
        # RankingEvaluationSystem 인스턴스 생성
        ranking_system = ranking_modules['ranking'].RankingEvaluationSystem()
        
        stats = {
            "ranking_types": {
                "same_job_rank": "동일 직군+연차 내 순위",
                "organization_rank": "팀/조직 내 순위"
            },
            "database_tables": {
                "source": "user_quarter_scores (MariaDB)",
                "storage": "ranking_results (MongoDB)"
            },
            "ranking_criteria": {
                "same_job": "job_id + job_years 기준",
                "organization": "organization_id 기준",
                "score_basis": "final_score 내림차순"
            },
            "modules_available": {
                "ranking_evaluation": ranking_modules.get('ranking') is not None,
                "comprehensive_report": ranking_modules.get('report') is not None
            },
            "features": [
                "실시간 랭킹 계산",
                "MongoDB 자동 저장",
                "분기별 데이터 관리",
                "직군별/팀별 랭킹 제공",
                "종합 성과 리포트 생성"
            ],
            "source_files": ["ranking_evaluation_agent.py", "generate_quarterly_report.py"]
        }
        
        # DB 연결이 가능하면 추가 통계 조회
        try:
            conn = ranking_system.get_db_connection()
            cursor = conn.cursor()
            
            # 2024년 4분기 기준 통계
            cursor.execute("""
                SELECT COUNT(DISTINCT user_id) as total_users
                FROM user_quarter_scores 
                WHERE evaluation_year = 2024 
                AND evaluation_quarter = 4
                AND final_score IS NOT NULL
                AND user_rank IS NOT NULL
                AND team_rank IS NOT NULL
            """)
            
            result = cursor.fetchone()
            if result:
                stats["current_stats"] = {
                    "total_ranked_users_2024_q4": result['total_users'],
                    "data_source": "user_quarter_scores table"
                }
            
            conn.close()
        except Exception as e:
            logger.warning(f"DB 통계 조회 실패: {e}")
        
        return {
            "success": True,
            "service": "ranking_report",
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Ranking 통계 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=f"Ranking 통계 조회 실패: {str(e)}")


# ===== 라우터 등록 =====
app.include_router(base_router)
app.include_router(weekly_report_router)
app.include_router(score_router)
app.include_router(ranking_router)


# 메인 실행부
if __name__ == "__main__":
    import uvicorn
    
    print("🚀 AI 성과관리 시스템 API 서버 시작")
    print("📝 API 문서: http://localhost:8000/docs")
    print("🎯 주간 보고서 평가: http://localhost:8000/api/v1/ai/weekly-report/evaluate")
    print("👥 사용자 목록: http://localhost:8000/api/v1/ai/weekly-report/users")
    print("🔧 기본 API: http://localhost:8000/api/v1/ai/hello")
    print("💊 Health Check: http://localhost:8000/health")