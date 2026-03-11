import json
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from pipeline.stage_02_analyze import SemanticAnalyzer
from utils.llm_client import LLMResponse, LLMError

# Fixture pointing to the Stage 01 output
@pytest.fixture
def test_structural_file():
    return Path("outputs/structural_2026-03-11T06-25-08.json")

def generate_mock_json(finding_type="mixed_sentinel", hypothesis="no_sales_status", conf=0.92, include_unknown=False, sheet="BCG", field="Продажи"):
    data = {
        "resolved": [],
        "confirm_queue": [],
        "escalate_queue": []
    }
    
    item = {
        "finding_type": finding_type,
        "sheet": sheet,
        "field": field,
        "hypothesis": hypothesis,
        "confidence": conf,
        "evidence": ["test evidence"]
    }
    
    if include_unknown:
        data["resolved"].append({
            "finding_type": "sheet_role",
            "sheet": "unknown sheet",
            "hypothesis": "unknown",
            "confidence": 0.99,
            "evidence": ["idk"]
        })
        
    data["resolved"].append(item)
    return json.dumps(data)

@pytest.mark.asyncio
async def test_sentinel_classification(test_structural_file):
    # Tests that the override for План Антонова correctly sets confidence to 0.95 and routes to resolved
    analyzer = SemanticAnalyzer(anthropic_key="test", openai_key="test")
    
    # Mock LLM placing it in escalate_queue with low confidence
    mock_json = json.dumps({
        "resolved": [],
        "confirm_queue": [],
        "escalate_queue": [{
            "finding_type": "mixed_sentinel",
            "sheet": "План Антонова",
            "field": "All",
            "hypothesis": "unknown",
            "confidence": 0.1,
            "evidence": ["looks like pivot"]
        }]
    })
    
    with patch("utils.llm_client.AnthropicClient.complete", new_callable=AsyncMock) as mock_anthropic:
        mock_anthropic.return_value = LLMResponse(
            provider="anthropic", model="test-model", content=mock_json,
            prompt_tokens=10, completion_tokens=10, input_hash="hash"
        )
        
        result = await analyzer.analyze(test_structural_file, strategy="single_anthropic")
        
        # Override should have caught it and forced it into resolved
        assert len(result["resolved"]) == 1
        assert result["resolved"][0]["hypothesis"] == "filter_artifact"
        assert result["resolved"][0]["confidence"] == 0.95

@pytest.mark.asyncio
async def test_unknown_always_escalates(test_structural_file):
    analyzer = SemanticAnalyzer(anthropic_key="test", openai_key="test")
    
    # Send high confidence 'unknown'
    mock_json = generate_mock_json(include_unknown=True)
    
    with patch("utils.llm_client.AnthropicClient.complete", new_callable=AsyncMock) as mock_anthropic:
        mock_anthropic.return_value = LLMResponse(
            provider="anthropic", model="test-model", content=mock_json,
            prompt_tokens=10, completion_tokens=10, input_hash="hash"
        )
        
        result = await analyzer.analyze(test_structural_file, strategy="single_anthropic")
        
        # 'unknown' finding MUST be in escalate
        escalated = [e for e in result["escalate_queue"] if e["hypothesis"] == "unknown"]
        assert len(escalated) == 1
        assert "question_for_human" in escalated[0]

@pytest.mark.asyncio
async def test_compete_mode(test_structural_file):
    analyzer = SemanticAnalyzer(anthropic_key="test", openai_key="test")
    
    # Anthropic returns high confidence
    anthropic_json = generate_mock_json(conf=0.90)
    # OpenAI returns lower confidence
    openai_json = generate_mock_json(conf=0.85)
    
    with patch("utils.llm_client.AnthropicClient.complete", new_callable=AsyncMock) as mock_ant:
        with patch("utils.llm_client.OpenAIClient.complete", new_callable=AsyncMock) as mock_oai:
            
            mock_ant.return_value = LLMResponse(
                provider="anthropic", model="test", content=anthropic_json,
                prompt_tokens=10, completion_tokens=10, input_hash="hash1"
            )
            mock_oai.return_value = LLMResponse(
                provider="openai", model="test", content=openai_json,
                prompt_tokens=10, completion_tokens=10, input_hash="hash2"
            )
            
            result = await analyzer.analyze(test_structural_file, strategy="compete")
            
            assert result["winner_provider"] == "anthropic"
            assert result["compete_log"]["anthropic_confidence_sum"] > result["compete_log"]["openai_confidence_sum"]

@pytest.mark.asyncio
async def test_fallback_on_error(test_structural_file):
    analyzer = SemanticAnalyzer(anthropic_key="test", openai_key="test")
    openai_json = generate_mock_json(conf=0.85)

    with patch("utils.llm_client.AnthropicClient.complete", new_callable=AsyncMock) as mock_ant:
        with patch("utils.llm_client.OpenAIClient.complete", new_callable=AsyncMock) as mock_oai:
            
            mock_ant.side_effect = LLMError("Anthropic API Down")
            mock_oai.return_value = LLMResponse(
                provider="openai", model="test", content=openai_json,
                prompt_tokens=10, completion_tokens=10, input_hash="hash2"
            )
            
            result = await analyzer.analyze(test_structural_file, strategy="compete")
            
            # Winner is whichever survived
            # Note: The pipeline degrade logic might set winner manually or rely on keys. 
            # In our degrade implementation winner_provider becomes the dictionary key (openai).
            # The compete log might be None if degrade occurs.
            assert result["compete_log"] is None
            assert result["winner_provider"] == "openai"
            assert len(result["resolved"]) == 1
