import asyncio
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import logging
import re
import time

# For web scraping
import requests
from bs4 import BeautifulSoup

# For PDF processing
try:
    import fitz  # PyMuPDF
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("Warning: PyMuPDF not installed. PDF processing will be simulated.")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pyq_scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ConfigurationManager:
    """Agent 1: Manages all configurations"""
    
    def __init__(self, config_path: str = "config.json"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        if self.config_path.exists():
            with open(self.config_path, 'r') as f:
                return json.load(f)
        else:
            logger.error("config.json not found!")
            return {}
    
    def get_url(self, board: str, class_num: str, subject: str, year: str) -> str:
        base = self.config["boards"][board]["base_url"]
        return f"{base}/previous-year-question-papers-class-{class_num}/{subject}/question-paper-{year}/"


class WebScraper:
    
    def __init__(self, config: ConfigurationManager):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.config["scraping"]["user_agent"]
        })
    
    def scrape_paper_links(self, url: str, year: str) -> List[Dict]:
        logger.info(f"[SCRAPING] {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            papers = []
            
            all_links = soup.find_all('a', href=True)
            
            for link in all_links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                # Check if it's a Google Drive link
                if 'drive.google.com' in href:
                    # Try to extract set number from the link text
                    # Looking for patterns like "30-1-1", "30-1-2", etc.
                    set_match = re.search(r'(\d+-\d+-\d+)', text)
                    
                    if set_match:
                        set_id = set_match.group(1)
                        download_url = self._convert_to_download_link(href)
                        
                        papers.append({
                            'year': year,
                            'set': set_id,
                            'text': text,
                            'view_url': href,
                            'download_url': download_url,
                            'status': 'pending'
                        })
                        logger.info(f"   [FOUND] {set_id} -> {download_url[:50]}...")
            
            # If no matches found with text, try finding all drive links
            if not papers:
                logger.warning("   No set numbers found in link text, trying alternative method...")
                drive_links = [link for link in all_links if 'drive.google.com' in link.get('href', '')]
                
                for idx, link in enumerate(drive_links[:9], 1):  # Max 9 papers per year
                    href = link.get('href', '')
                    # Generate set IDs assuming standard pattern
                    set_num = ((idx - 1) // 3) + 1
                    subset_num = ((idx - 1) % 3) + 1
                    set_id = f"30-{set_num}-{subset_num}"
                    
                    download_url = self._convert_to_download_link(href)
                    
                    papers.append({
                        'year': year,
                        'set': set_id,
                        'text': link.get_text(strip=True),
                        'view_url': href,
                        'download_url': download_url,
                        'status': 'pending'
                    })
                    logger.info(f"   ✓ Found (inferred): {set_id}")
            
            logger.info(f"[TOTAL] Papers found for {year}: {len(papers)}")
            return papers
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return []
    
    def _convert_to_download_link(self, drive_url: str) -> str:
        file_id = None
        
        patterns = [
            r'/file/d/([^/]+)',           # /file/d/FILE_ID/view
            r'id=([^&]+)',                 # ?id=FILE_ID
            r'/open\?id=([^&]+)',         # /open?id=FILE_ID
            r'/d/([^/]+)',                # /d/FILE_ID
        ]
        
        for pattern in patterns:
            match = re.search(pattern, drive_url)
            if match:
                file_id = match.group(1)
                break
        
        if file_id:
            # Return direct download URL
            return f"https://drive.google.com/uc?export=download&id={file_id}"
        
        logger.warning(f"Could not extract file ID from: {drive_url}")
        return drive_url
    
    def download_pdf(self, paper: Dict, output_dir: Path) -> Dict:
        """Download PDF from Google Drive"""
        try:
            # Create directory structure
            paper_dir = output_dir / "cbse" / "10" / "maths" / paper["year"]
            paper_dir.mkdir(parents=True, exist_ok=True)
            
            filename = f"{paper['set']}.pdf"
            filepath = paper_dir / filename
            
            # Skip if already downloaded
            if filepath.exists() and filepath.stat().st_size > 0:
                logger.info(f"   [SKIP] Already downloaded: {filename}")
                paper["local_path"] = str(filepath)
                paper["status"] = "downloaded"
                return paper
            
            logger.info(f"   [DOWNLOAD] {paper['year']} - {paper['set']}...")
            
            # Download with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = self.session.get(
                        paper['download_url'], 
                        timeout=60,
                        stream=True,
                        allow_redirects=True
                    )
                    
                    # Check if we got a PDF
                    content_type = response.headers.get('content-type', '').lower()
                    
                    if response.status_code == 200:
                        # Save PDF
                        with open(filepath, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                        
                        # Verify file size
                        if filepath.stat().st_size > 1000: 
                            logger.info(f"      [OK] Downloaded: {filepath.stat().st_size / 1024:.1f} KB")
                            paper["local_path"] = str(filepath)
                            paper["status"] = "downloaded"
                            paper["download_time"] = datetime.now().isoformat()
                            return paper
                        else:
                            logger.warning(f"      File too small, retrying...")
                            filepath.unlink(missing_ok=True)
                    
                except Exception as e:
                    logger.warning(f"      Attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  
                    continue
            
            # All retries failed
            logger.error(f"Failed to download after {max_retries} attempts")
            paper["status"] = "failed"
            paper["error"] = "Download failed"
            return paper
            
        except Exception as e:
            logger.error(f"Error downloading {paper['set']}: {e}")
            paper["status"] = "failed"
            paper["error"] = str(e)
            return paper


class PDFParser:
    """Agent 3: Extracts text from PDFs"""
    
    def __init__(self, config: ConfigurationManager):
        self.config = config
    
    def extract_text(self, pdf_path: Path) -> str:
        """Extract text from PDF using PyMuPDF"""
        if not PDF_AVAILABLE:
            logger.warning(f"PyMuPDF not available, using simulated extraction")
            return self._simulate_extraction()
        
        try:
            logger.info(f"   [EXTRACT] Text from: {pdf_path.name}")
            
            doc = fitz.open(pdf_path)
            full_text = ""
            
            for page_num in range(len(doc)):
                page = doc[page_num]
                text = page.get_text()
                full_text += text + "\n"
            
            doc.close()
            
            if text and len(text) > 100:
                logger.info(f"      [OK] Extracted {len(text)} characters")
                
                debug_file = Path("debug_extracted_text.txt")
                if not debug_file.exists():
                    with open(debug_file, 'w', encoding='utf-8') as f:
                        f.write(f"=== Sample from {pdf_path.name} ===\n\n")
                        f.write(text[:2000])  # First 2000 chars
                        f.write("\n\n=== END SAMPLE ===")
                    logger.info(f"      [DEBUG] Saved sample to debug_extracted_text.txt")
            else:
                logger.info(f"      [OK] Extracted {len(full_text)} characters")
            
            return full_text
            
        except Exception as e:
            logger.error(f"Error extracting text: {e}")
            return ""
    
    def _simulate_extraction(self) -> str:
        """Fallback simulation for testing"""
        return "demo"
    
    def segment_questions(self, text: str) -> List[Dict]:
        """Parse text into individual questions - handles multiple formats"""
        questions = []
        
        patterns = [
            r'Q\.?\s*(\d+)[\.\)]\s+(.+?)(?=Q\.?\s*\d+[\.\)]|$)',  # Q.1. or Q1) or Q 1.
            r'(\d+)[\.\)]\s+(.+?)(?=\d+[\.\)]|$)',                 # 1. or 1)
            r'Question\s+(\d+)[:\.\)]\s*(.+?)(?=Question\s+\d+|$)', # Question 1:
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
            if matches and len(matches) > 5:  # Found at least 5 questions
                for q_num, q_text in matches:
                    q_text = q_text.strip()
                    
                    # Skip if text is too short (likely header/footer)
                    if len(q_text) < 10:
                        continue
                    
                    marks = 1  
                    marks_patterns = [
                        r'\[(\d+)\s*marks?\]',
                        r'\((\d+)\s*marks?\)',
                        r'(\d+)\s*marks?',
                        r'\[(\d+)m\]',
                        r'\((\d+)m\)',
                    ]
                    
                    for mp in marks_patterns:
                        marks_match = re.search(mp, q_text, re.IGNORECASE)
                        if marks_match:
                            marks = int(marks_match.group(1))
                            break
                    
                    questions.append({
                        "question_number": int(q_num),
                        "text": q_text[:500],  
                        "marks": marks
                    })
                
                if questions:
                    break  
        
        logger.info(f"      [OK] Parsed {len(questions)} questions")
        return questions


class NLPAnalyzer:
    """Agent 4: Analyzes questions"""
    
    def __init__(self, config: ConfigurationManager):
        self.config = config
        self.topic_keywords = self._load_topic_keywords()
    
    def _load_topic_keywords(self) -> Dict:
        return {
            "Number Systems": ["hcf", "lcm", "euclid", "irrational", "rational", "prime", "composite"],
            "Algebra": ["polynomial", "quadratic", "equation", "linear", "zeros", "coefficient"],
            "Coordinate Geometry": ["distance", "section formula", "midpoint", "coordinates", "slope"],
            "Geometry": ["triangle", "circle", "tangent", "chord", "radius", "similar", "congruent"],
            "Trigonometry": ["sin", "cos", "tan", "elevation", "depression", "height and distance"],
            "Mensuration": ["volume", "surface area", "cone", "cylinder", "sphere", "frustum"],
            "Statistics": ["mean", "median", "mode", "frequency", "distribution"],
            "Probability": ["probability", "random", "event", "outcome"]
        }
    
    def analyze_question(self, question: Dict) -> Dict:
        """Analyze a single question"""
        text_lower = question["text"].lower()
        
        # Classify type
        question["type"] = self._classify_type(question)
        
        # Estimate difficulty
        question["difficulty"] = self._estimate_difficulty(question)
        
        # Extract topics
        question["topics"] = self._extract_topics(text_lower)
        
        return question
    
    def _classify_type(self, q: Dict) -> str:
        text = q["text"].lower()
        marks = q["marks"]
        
        if any(word in text for word in ["choose", "select", "tick", "(a)", "(b)"]):
            return "MCQ"
        elif marks == 1:
            return "MCQ"
        elif marks == 2:
            return "VSA"
        elif marks == 3:
            return "SA"
        else:
            return "LA"
    
    def _estimate_difficulty(self, q: Dict) -> str:
        text = q["text"].lower()
        marks = q["marks"]
        
        easy_kw = ["state", "define", "list", "name", "identify", "write"]
        medium_kw = ["explain", "describe", "calculate", "find", "solve"]
        hard_kw = ["prove", "derive", "analyze", "justify", "show that"]
        
        score = 0
        if any(kw in text for kw in easy_kw): score += 1
        if any(kw in text for kw in medium_kw): score += 2
        if any(kw in text for kw in hard_kw): score += 3
        
        score += min(marks, 3)
        
        if score <= 3: return "Easy"
        elif score <= 6: return "Medium"
        else: return "Hard"
    
    def _extract_topics(self, text: str) -> List[str]:
        topics = []
        for topic, keywords in self.topic_keywords.items():
            if any(kw in text for kw in keywords):
                topics.append(topic)
        return topics if topics else ["General"]


class PatternGenerator:
    """Agent 5: Generates analytics"""
    
    def __init__(self, config: ConfigurationManager):
        self.config = config
    
    def generate_analytics(self, all_questions: List[Dict]) -> Dict:
        analytics = {
            "metadata": {
                "total_questions": len(all_questions),
                "generated_at": datetime.now().isoformat(),
                "years": sorted(list(set(q.get("year") for q in all_questions if q.get("year"))))
            },
            "difficulty_distribution": self._calc_difficulty(all_questions),
            "type_distribution": self._calc_types(all_questions),
            "topic_frequency": self._calc_topics(all_questions),
            "year_wise_analysis": self._calc_yearly(all_questions),
            "repeated_concepts": self._find_repeated(all_questions)
        }
        return analytics
    
    def _calc_difficulty(self, questions):
        dist = {"Easy": 0, "Medium": 0, "Hard": 0}
        for q in questions:
            dist[q.get("difficulty", "Medium")] += 1
        total = sum(dist.values())
        if total > 0:
            dist["percentages"] = {k: round((v/total)*100, 1) for k, v in dist.items()}
        return dist
    
    def _calc_types(self, questions):
        dist = {}
        for q in questions:
            t = q.get("type", "Unknown")
            dist[t] = dist.get(t, 0) + 1
        return dist
    
    def _calc_topics(self, questions):
        topics = {}
        for q in questions:
            for topic in q.get("topics", []):
                if topic not in topics:
                    topics[topic] = {"count": 0, "years": set()}
                topics[topic]["count"] += 1
                topics[topic]["years"].add(q.get("year"))
        
        for topic in topics:
            topics[topic]["years"] = sorted(list(topics[topic]["years"]))
            topics[topic]["frequency"] = len(topics[topic]["years"])
        
        return topics
    
    def _calc_yearly(self, questions):
        years = {}
        for q in questions:
            year = q.get("year")
            if year not in years:
                years[year] = {"count": 0, "difficulty": {"Easy": 0, "Medium": 0, "Hard": 0}}
            years[year]["count"] += 1
            years[year]["difficulty"][q.get("difficulty", "Medium")] += 1
        return years
    
    def _find_repeated(self, questions):
        topics = self._calc_topics(questions)
        repeated = [
            {"topic": t, **data}
            for t, data in topics.items()
            if data["frequency"] >= 2  # Appeared in 2+ years
        ]
        return sorted(repeated, key=lambda x: x["count"], reverse=True)
    
    def save_analytics(self, analytics: Dict, output_path: Path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(analytics, f, indent=2, ensure_ascii=False)
        logger.info(f"[SAVED] Analytics saved to: {output_path}")


# Main Orchestrator
class PYQAnalyzer:
    def __init__(self, config_path: str = "config.json"):
        self.config = ConfigurationManager(config_path)
        self.scraper = WebScraper(self.config)
        self.parser = PDFParser(self.config)
        self.nlp = NLPAnalyzer(self.config)
        self.pattern_gen = PatternGenerator(self.config)
    
    def analyze(self, board: str = "cbse", class_num: str = "10", subject: str = "maths"):
        print("\n" + "="*60)
        print("Multi-Year Analysis Started")
        print("="*60)
        
        years = self.config.config.get("years", ["2025", "2024"])
        print(f"Subject: {subject.title()}")
        print(f"Class: {class_num}")
        print(f"Analyzing Years: {years[0]} to {years[-1]}")
        print(f"Source: Oswal Publishers")
        print("="*60 + "\n")
        
        all_questions = []
        output_dir = Path(self.config.config["output"]["pdf_dir"])
        
        for year in years:
            print(f"\nProcessing Year: {year}")
            print("-" * 60)
            
            url = self.config.get_url(board, class_num, subject, year)
            papers = self.scraper.scrape_paper_links(url, year)
            
            if not papers:
                print(f"No papers found for {year}, skipping...")
                continue
            
            print(f"\n⬇Downloading {len(papers)} PDFs...")
            
            for paper in papers:
                downloaded = self.scraper.download_pdf(paper, output_dir)
                
                if downloaded["status"] == "downloaded":
                    pdf_path = Path(downloaded["local_path"])
                    text = self.parser.extract_text(pdf_path)
                    
                    if text and len(text) > 100:
                        questions = self.parser.segment_questions(text)
                        
                        for q in questions:
                            q["year"] = year
                            q["paper_set"] = paper["set"]
                            analyzed = self.nlp.analyze_question(q)
                            all_questions.append(analyzed)
                
                time.sleep(2)  
        
        if all_questions:
            print(f"\n\n{'='*60}")
            print(f"Generating Analytics...")
            print(f"{'='*60}\n")
            
            analytics = self.pattern_gen.generate_analytics(all_questions)
            
            output_file = Path(self.config.config["output"]["analytics_dir"]) / \
                         f"{board}_class{class_num}_{subject}_analytics.json"
            self.pattern_gen.save_analytics(analytics, output_file)
            
            self._print_summary(analytics)
            
            return analytics
        else:
            print("\nNo papers could be analyzed – either Drive downloads failed or PDFs have no extractable text.")
            return None
    
    def _print_summary(self, analytics):
        print(f"\n{'='*60}")
        print("ANALYSIS COMPLETE!")
        print(f"{'='*60}\n")
        
        meta = analytics["metadata"]
        print(f"Total Questions Analyzed: {meta['total_questions']}")
        print(f"Years Covered: {', '.join(str(y) for y in meta['years'])}\n")
        
        print("Difficulty Distribution:")
        for level, pct in analytics["difficulty_distribution"].get("percentages", {}).items():
            bar = "█" * int(pct / 2)
            print(f"   {level:8} {pct:5.1f}% {bar}")
        
        print(f"\nTop 5 Most Important Topics:")
        topics = sorted(
            analytics["topic_frequency"].items(),
            key=lambda x: x[1]["count"],
            reverse=True
        )[:5]
        
        for i, (topic, data) in enumerate(topics, 1):
            print(f"   {i}. {topic:25} {data['count']:3} questions ({data['frequency']} years)")
        
        print(f"\nRepeated Concepts (2+ years): {len(analytics['repeated_concepts'])}")
        
        print(f"\n{'='*60}\n")


def main():
    try:
        analyzer = PYQAnalyzer()
        analytics = analyzer.analyze()
        
        if analytics:
            print("Success! Check data/analytics/ folder for JSON output")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"\nError: {e}")
        print("Check pyq_scraper.log for details")


if __name__ == "__main__":
    main()