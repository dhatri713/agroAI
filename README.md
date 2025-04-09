# AgroAI 🌾 — Voice-Powered Crop Assistant

AgroAI helps farmers get instant answers about crop diseases and pests using advanced AI, voice input, and retrieval-augmented generation.

---

## 🚀 Features
- Telugu language integration so that it is more accessible to farmers
- Voice-based Question Answering
- AI-powered Crop Advisory
- Supports multiple crops & diseases
- Fast, accurate responses using ChromaDB
- Uses OpenAI & Groq LLM APIs
- Simple frontend with TailwindCSS

---

## 🛠️ Tech Stack
- Python
- NodeJS
- ChromaDB
- Sentence Transformers
- OpenAI / Groq APIs
- TailwindCSS
- Axios

---

## ⚙️ Installation

### Clone the Repository
```bash
git clone <your-repo-url>
cd AgroAI
```

### Python Setup
```bash
cd backend
python -m venv venv
source venv/bin/activate   # For Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### NodeJS Setup (Frontend or Voice Support)
```bash
cd frontend
npm install
```

---

## 🗝️ Environment Variables

Create a `.env` file in root directory based on `.env.example`:

```
OPENAI_API_KEY=your_value_here
GROQ_API_KEY=your_value_here
CHROMA_DB_PATH=your_value_here
```

---

## 🚀 Running the Project

### Backend (Python)
```bash
cd backend
python app.py  # or main.py or whatever your entry file is
```

### Frontend (Node/JS)
```bash
cd frontend
npm start  # or npm run dev
```

---

## 📁 Folder Structure

```
AgroAI/
├── backend/               ← Python retrieval & AI logic
├── frontend/              ← HTML / Tailwind / JS
├── .env                   ← Environment variables (DO NOT COMMIT)
├── .env.example           ← Safe sample
├── requirements.txt       ← Python dependencies
├── package.json           ← NodeJS dependencies
└── README.md
```

---

## 📝 License
MIT

## ❤️ Credits
Built with love for farmers 🌾
