# tools/ollama_client.py
"""
Ollama LLM client for CV analysis and job matching.
Uses local Ollama instance for privacy and speed.
"""
import json
import requests
from typing import Dict, List, Optional, Tuple


OLLAMA_API_URL = "http://localhost:11434/api/generate"
DEFAULT_MODEL = "llama3:latest"


def test_connection(model: str = DEFAULT_MODEL) -> bool:
    """Test if Ollama is running and model is available."""
    try:
        response = requests.post(
            OLLAMA_API_URL,
            json={
                "model": model,
                "prompt": "Hello",
                "stream": False
            },
            timeout=10
        )
        return response.status_code == 200
    except Exception as e:
        print(f"Ollama connection failed: {e}")
        return False


def extract_skills_from_cv(cv_text: str, model: str = DEFAULT_MODEL) -> Dict:
    """
    Extract skills, experience, and profile from CV using Ollama.
    
    Args:
        cv_text: Full text of the CV
        model: Ollama model to use
        
    Returns:
        Dict with skills, years_experience, domain, roles
    """
    prompt = f"""Analyze this CV and extract information in JSON format.

CV Text:
{cv_text[:3000]}

Extract:
1. technical_skills: List of technical skills (programming languages, tools, frameworks)
2. years_experience: Total years of professional experience (integer)
3. domain_expertise: Domain/industry expertise areas
4. preferred_roles: Types of roles they seem suited for
5. education: Highest education level
6. location_preference: Preferred work locations mentioned

Return ONLY valid JSON, no other text:
{{
  "technical_skills": ["Python", "SQL", "Machine Learning"],
  "years_experience": 3,
  "domain_expertise": ["Finance", "Data Analysis"],
  "preferred_roles": ["Data Analyst", "Data Engineer"],
  "education": "Bachelor's in Computer Science",
  "location_preference": ["Bengaluru", "Mumbai", "Remote"]
}}
"""
    
    try:
        response = requests.post(
            OLLAMA_API_URL,
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.3,  # Lower temperature for more consistent extraction
                }
            },
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            response_text = result.get("response", "{}")
            
            # Try to parse JSON from response
            try:
                # Extract JSON from response (handle extra text)
                response_text_clean = response_text
                
                # Try to find JSON object in the text
                json_start_idx = response_text.find('{')
                json_end_idx = response_text.rfind('}')
                
                if json_start_idx != -1 and json_end_idx != -1 and json_end_idx > json_start_idx:
                    response_text_clean = response_text[json_start_idx:json_end_idx+1]
                
                skills_data = json.loads(response_text_clean)
                return skills_data
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON response: {e}")
                print(f"Response was: {response_text[:500]}")
                return {
                    "technical_skills": [],
                    "years_experience": 0,
                    "domain_expertise": [],
                    "preferred_roles": [],
                    "education": "",
                    "location_preference": []
                }
        else:
            print(f"Ollama API error: {response.status_code}")
            return {}
            
    except Exception as e:
        print(f"Error extracting skills: {e}")
        return {}


def match_job_to_cv(
    cv_summary: Dict,
    job_title: str,
    job_description: str,
    job_location: str = "",
    model: str = DEFAULT_MODEL
) -> Tuple[int, str]:
    """
    Match a job against CV using Ollama.
    
    Args:
        cv_summary: Dict with extracted CV info
        job_title: Job title
        job_description: Job description
        job_location: Job location
        model: Ollama model to use
        
    Returns:
        (score: int 0-100, reasoning: str)
    """
    # Build CV summary string
    cv_text = f"""
CV Summary:
- Skills: {', '.join(cv_summary.get('technical_skills', [])[:10])}
- Experience: {cv_summary.get('years_experience', 0)} years
- Domain: {', '.join(cv_summary.get('domain_expertise', []))}
- Preferred Roles: {', '.join(cv_summary.get('preferred_roles', []))}
- Education: {cv_summary.get('education', 'Not specified')}
- Location Preference: {', '.join(cv_summary.get('location_preference', []))}
""".strip()
    
    # Truncate description if too long
    desc = (job_description or "")[:500]
    
    prompt = f"""You are a career advisor. Rate how well this job matches the candidate's profile.

{cv_text}

Job to Evaluate:
- Title: {job_title}
- Location: {job_location}
- Description: {desc}

Provide:
1. Match score (0-100): How well does this job match the candidate?
2. Reasoning: Brief explanation of why it matches or doesn't

Return ONLY valid JSON:
{{
  "score": 85,
  "reasoning": "Strong match: Job requires Python and SQL which candidate has. Experience level matches. Location is preferred city."
}}
"""
    
    try:
        response = requests.post(
            OLLAMA_API_URL,
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.2,
                }
            },
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            response_text = result.get("response", "{}")
            
            # Parse JSON
            try:
                # Extract JSON from response (handle extra text)
                response_text_clean = response_text
                
                # Try to find JSON object in the text
                json_start_idx = response_text.find('{')
                json_end_idx = response_text.rfind('}')
                
                if json_start_idx != -1 and json_end_idx != -1 and json_end_idx > json_start_idx:
                    response_text_clean = response_text[json_start_idx:json_end_idx+1]
                
                match_data = json.loads(response_text_clean)
                score = int(match_data.get("score", 0))
                reasoning = match_data.get("reasoning", "No reasoning provided")
                
                # Clamp score to 0-100
                score = max(0, min(100, score))
                
                return score, reasoning
                
            except (json.JSONDecodeError, ValueError) as e:
                print(f"Failed to parse match response: {e}")
                print(f"Response was: {response_text[:500]}")
                return 0, "Error parsing AI response"
        else:
            print(f"Ollama API error: {response.status_code}")
            return 0, "API error"
            
    except Exception as e:
        print(f"Error matching job: {e}")
        return 0, f"Error: {str(e)}"


if __name__ == "__main__":
    # Test connection
    print("Testing Ollama connection...")
    if test_connection():
        print("✓ Ollama is running and accessible")
        
        # Test skill extraction with sample CV
        sample_cv = """
        John Doe
        Senior Data Analyst
        
        Experience: 3 years in financial data analysis
        
        Skills:
        - Python (Pandas, NumPy, Scikit-learn)
        - SQL (PostgreSQL, MySQL)
        - Machine Learning (Classification, Regression)
        - Data Visualization (Tableau, Matplotlib)
        - AWS (S3, EC2)
        
        Education: Bachelor's in Computer Science
        
        Preferred Location: Bengaluru, Mumbai
        """
        
        print("\nExtracting skills from sample CV...")
        skills = extract_skills_from_cv(sample_cv)
        print(json.dumps(skills, indent=2))
        
        # Test job matching
        print("\nMatching sample job...")
        score, reasoning = match_job_to_cv(
            cv_summary=skills,
            job_title="Data Engineer - Python & SQL",
            job_description="Looking for data engineer with 2-4 years experience in Python, SQL, and cloud platforms. Will work with financial data.",
            job_location="Bengaluru, India"
        )
        print(f"Match Score: {score}/100")
        print(f"Reasoning: {reasoning}")
    else:
        print("✗ Ollama is not running. Start it with: ollama serve")
