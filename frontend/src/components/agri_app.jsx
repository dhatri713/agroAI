import React, { useState, useRef, useEffect } from 'react';
import axios from 'axios';

const API_URL = 'http://localhost:8000'; // Your FastAPI backend URL

// Telugu translations for UI elements
const translations = {
    english: {
        title: "AgroAI",
        newChat: "New Chat",
        welcomeMessage: "Welcome to AgroAI - Your agricultural knowledge partner",
        inputPlaceholder: "Ask about agriculture in English...",
        errorMessage: "Sorry, I encountered an error processing your request.",
        audioError: "Sorry, I encountered an error processing your audio.",
        microphoneError: "I couldn't access your microphone. Please check your browser permissions.",
        voiceMessage: "Voice message"
    },
    telugu: {
        title: "అగ్రోఏఐ", // Changed to AgroAI in Telugu script
        newChat: "కొత్త చాట్",
        welcomeMessage: "అగ్రోఏఐ - మీ వ్యవసాయ జ్ఞాన భాగస్వామికి స్వాగతం",
        inputPlaceholder: "తెలుగులో వ్యవసాయం గురించి అడగండి...",
        errorMessage: "క్షమించండి, మీ అభ్యర్థనను ప్రాసెస్ చేయడంలో లోపం ఎదురైంది.",
        audioError: "క్షమించండి, మీ ఆడియోను ప్రాసెస్ చేయడంలో లోపం ఎదురైంది.",
        microphoneError: "మీ మైక్రోఫోన్‌ను యాక్సెస్ చేయలేకపోయాను. దయచేసి మీ బ్రౌజర్ అనుమతులను తనిఖీ చేయండి.",
        voiceMessage: "వాయిస్ సందేశం"
    }
};

function AgroAI() {
    const [query, setQuery] = useState('');
    const [selectedLanguage, setSelectedLanguage] = useState('english');
    const [isRecording, setIsRecording] = useState(false);
    const [loading, setLoading] = useState(false);
    const [messages, setMessages] = useState([]);
    const [conversationId, setConversationId] = useState(null);
    const [transcribedText, setTranscribedText] = useState('');
    const [isPlaying, setIsPlaying] = useState(false);
    const [currentAudio, setCurrentAudio] = useState(null);
    const [audioProgress, setAudioProgress] = useState(0);
    const [audioDuration, setAudioDuration] = useState(0);
    const [currentTime, setCurrentTime] = useState(0);

    const audioRef = useRef(null);
    const mediaRecorderRef = useRef(null);
    const audioChunksRef = useRef([]);
    const messagesEndRef = useRef(null);

    // Get translations based on selected language
    const t = translations[selectedLanguage];

    // Scroll to bottom of messages
    useEffect(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }, [messages]);

    // Generate a random conversation ID when the component mounts
    useEffect(() => {
        startNewChat();
    }, []);

    // Start a new chat when language changes
    useEffect(() => {
        if (conversationId) {
            startNewChat();
        }
    }, [selectedLanguage]);

    // Audio event listeners
    useEffect(() => {
        const audioElement = audioRef.current;

        const handlePlay = () => setIsPlaying(true);
        const handlePause = () => setIsPlaying(false);
        const handleEnded = () => {
            setIsPlaying(false);
            setAudioProgress(0);
            setCurrentTime(0);
        };
        const handleTimeUpdate = () => {
            if (audioElement) {
                setCurrentTime(audioElement.currentTime);
                setAudioProgress((audioElement.currentTime / audioElement.duration) * 100);
            }
        };
        const handleLoadedMetadata = () => {
            if (audioElement) {
                setAudioDuration(audioElement.duration);
            }
        };

        if (audioElement) {
            audioElement.addEventListener('play', handlePlay);
            audioElement.addEventListener('pause', handlePause);
            audioElement.addEventListener('ended', handleEnded);
            audioElement.addEventListener('timeupdate', handleTimeUpdate);
            audioElement.addEventListener('loadedmetadata', handleLoadedMetadata);
        }

        return () => {
            if (audioElement) {
                audioElement.removeEventListener('play', handlePlay);
                audioElement.removeEventListener('pause', handlePause);
                audioElement.removeEventListener('ended', handleEnded);
                audioElement.removeEventListener('timeupdate', handleTimeUpdate);
                audioElement.removeEventListener('loadedmetadata', handleLoadedMetadata);
            }
        };
    }, [audioRef.current]);

    const startNewChat = () => {
        setMessages([]);
        setConversationId(`conv_${Math.random().toString(36).substring(2, 15)}`);
    };

    const base64ToBlob = (base64, mimeType) => {
        const byteCharacters = atob(base64);
        const byteArrays = [];
        for (let offset = 0; offset < byteCharacters.length; offset += 512) {
            const slice = byteCharacters.slice(offset, offset + 512);
            const byteNumbers = Array.from(slice).map(char => char.charCodeAt(0));
            byteArrays.push(new Uint8Array(byteNumbers));
        }
        return new Blob(byteArrays, { type: mimeType });
    };

    const formatTime = (timeInSeconds) => {
        if (isNaN(timeInSeconds)) return "0:00";
        const minutes = Math.floor(timeInSeconds / 60);
        const seconds = Math.floor(timeInSeconds % 60);
        return `${minutes}:${seconds < 10 ? '0' : ''}${seconds}`;
    };

    const playAudio = (audioUrl) => {
        if (currentAudio && currentAudio !== audioUrl) {
            audioRef.current.pause();
        }

        setCurrentAudio(audioUrl);
        audioRef.current.src = audioUrl;
        audioRef.current.play();
    };

    const togglePlayPause = (audioUrl) => {
        if (currentAudio !== audioUrl) {
            playAudio(audioUrl);
        } else {
            if (isPlaying) {
                audioRef.current.pause();
            } else {
                audioRef.current.play();
            }
        }
    };

    const seekAudio = (e, progressBarWidth) => {
        const clickPosition = e.nativeEvent.offsetX;
        const percent = clickPosition / progressBarWidth;
        const newTime = percent * audioDuration;

        if (audioRef.current) {
            audioRef.current.currentTime = newTime;
            setCurrentTime(newTime);
            setAudioProgress(percent * 100);
        }
    };

    const addMessage = (content, isUser, audioUrl = null, transcribedText = null) => {
        setMessages(prev => [...prev, {
            id: Date.now(),
            content,
            isUser,
            timestamp: new Date(),
            audioUrl,
            transcribedText
        }]);
    };

    const handleTextQuery = async () => {
        if (!query.trim()) return;

        // Add user message to chat
        addMessage(query, true);

        setLoading(true);
        setQuery(''); // Clear input field immediately after sending

        try {
            const response = await axios.post(`${API_URL}/query/text`, {
                query,
                language: selectedLanguage,
                conversation_id: conversationId
            });

            // Format the response content to remove markdown-style formatting
            if (response.data.answer) {
                response.data.answer = response.data.answer
                    .replace(/\*\*/g, '') // Remove bold asterisks
                    .replace(/\*/g, '')   // Remove single asterisks
                    .replace(/#/g, '')    // Remove heading symbols
                    .replace(/\d\.\s+/g, '$& '); // Add space after numbered list items
            }

            // Add bot response to chat
            let audioUrl = null;
            if (response.data.speech_base64) {
                const audioBlob = base64ToBlob(response.data.speech_base64, 'audio/mp3');
                audioUrl = URL.createObjectURL(audioBlob);
                setCurrentAudio(audioUrl);
                audioRef.current.src = audioUrl;
                audioRef.current.play();
            }

            addMessage(response.data.answer, false, audioUrl);
        } catch (error) {
            console.error('Text Query Error:', error);
            addMessage(t.errorMessage, false);
        } finally {
            setLoading(false);
        }
    };

    const handleKeyPress = (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            handleTextQuery();
        }
    };

    const handleRecord = async () => {
        if (isRecording) {
            mediaRecorderRef.current.stop();
            setIsRecording(false);
        } else {
            try {
                const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
                mediaRecorderRef.current = new MediaRecorder(stream);
                audioChunksRef.current = [];

                mediaRecorderRef.current.ondataavailable = (e) => {
                    if (e.data.size > 0) {
                        audioChunksRef.current.push(e.data);
                    }
                };

                mediaRecorderRef.current.onstop = async () => {
                    // Create a temporary URL for the recorded audio
                    const audioBlob = new Blob(audioChunksRef.current, { type: 'audio/wav' });
                    const tempAudioUrl = URL.createObjectURL(audioBlob);

                    setLoading(true);

                    const formData = new FormData();
                    formData.append('file', audioBlob);
                    formData.append('language', selectedLanguage);
                    formData.append('conversation_id', conversationId);

                    try {
                        const response = await axios.post(`${API_URL}/query/audio`, formData);

                        // Format the response
                        if (response.data.answer) {
                            response.data.answer = response.data.answer
                                .replace(/\*\*/g, '')
                                .replace(/\*/g, '')
                                .replace(/#/g, '')
                                .replace(/\d\.\s+/g, '$& ');
                        }

                        // Get the transcribed text from original query if available
                        let transcribed = response.data.original_query || t.voiceMessage;

                        // Add user message with audio and transcribed text
                        addMessage(t.voiceMessage, true, tempAudioUrl, transcribed);

                        // Add bot response with audio if available
                        let responseAudioUrl = null;
                        if (response.data.speech_base64) {
                            const audioBlob = base64ToBlob(response.data.speech_base64, 'audio/mp3');
                            responseAudioUrl = URL.createObjectURL(audioBlob);
                            setCurrentAudio(responseAudioUrl);
                            audioRef.current.src = responseAudioUrl;
                            audioRef.current.play();
                        }

                        addMessage(response.data.answer, false, responseAudioUrl);
                    } catch (error) {
                        console.error('Audio Query Error:', error);
                        addMessage(t.audioError, false);
                    } finally {
                        setLoading(false);
                    }

                    // Stop all tracks in the stream to release the microphone
                    stream.getTracks().forEach(track => track.stop());
                };

                mediaRecorderRef.current.start();
                setIsRecording(true);
            } catch (error) {
                console.error("Error accessing microphone:", error);
                addMessage(t.microphoneError, false);
            }
        }
    };

    // Custom audio player component with time display and interactive progress bar
    const AudioPlayer = ({ audioUrl }) => {
        const isCurrentPlaying = isPlaying && currentAudio === audioUrl;
        const progressBarRef = useRef(null);

        // Time display for this audio clip
        const displayTime = isCurrentPlaying
            ? `${formatTime(currentTime)} / ${formatTime(audioDuration)}`
            : "";

        const handleProgressBarClick = (e) => {
            if (currentAudio === audioUrl && progressBarRef.current) {
                const width = progressBarRef.current.clientWidth;
                seekAudio(e, width);
            }
        };

        return (
            <div className="mt-2 flex flex-col gap-1 p-2 rounded-md bg-indigo-900 bg-opacity-30">
                <div className="flex items-center gap-2">
                    <button
                        onClick={() => togglePlayPause(audioUrl)}
                        className="p-1.5 rounded-full bg-indigo-600 hover:bg-indigo-700 transition-colors"
                    >
                        {isCurrentPlaying ? (
                            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                <rect x="6" y="4" width="4" height="16"></rect>
                                <rect x="14" y="4" width="4" height="16"></rect>
                            </svg>
                        ) : (
                            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                <polygon points="5 3 19 12 5 21 5 3"></polygon>
                            </svg>
                        )}
                    </button>
                    <div
                        ref={progressBarRef}
                        className="w-full bg-indigo-800 rounded-full h-2 cursor-pointer relative"
                        onClick={handleProgressBarClick}
                    >
                        <div
                            className="bg-indigo-400 h-2 rounded-full transition-all duration-300"
                            style={{
                                width: currentAudio === audioUrl ? `${audioProgress}%` : '0%',
                                transition: isCurrentPlaying ? 'width 0.1s linear' : 'none'
                            }}
                        ></div>
                    </div>
                </div>

                {currentAudio === audioUrl && displayTime && (
                    <div className="text-xs text-indigo-300 text-right">
                        {displayTime}
                    </div>
                )}
            </div>
        );
    };

    return (
        <div className="relative min-h-screen w-full">
            {/* Dark theme background */}
            <div
                className="absolute inset-0 bg-indigo-950 z-0"
            />

            {/* Content */}
            <div className="relative z-10 p-4 md:p-8 flex flex-col items-center justify-center min-h-screen font-sans">
                <div className="bg-gray-900 rounded-xl shadow-lg max-w-3xl w-full p-4 flex flex-col h-[85vh] border border-indigo-800">
                    <div className="flex items-center gap-3 mb-4 p-2 border-b border-indigo-900">
                        {/* Logo */}
                        <div className="flex items-center justify-center w-10 h-10 bg-indigo-600 rounded-lg text-white font-bold">
                            <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                <path d="M12 2c.5 0 1 .2 1.4.6 2.4 2.4 5.4 4.6 5.4 4.6.7.5 1.2 1.3 1.2 2.2 0 1.5-1.3 2.7-2.7 2.6h-1.1c-1.1 0-2 .9-2 2v1.3c0 1.4.7 2.7 1.8 3.4.5.3.7.9.7 1.5 0 1-.8 1.8-1.8 1.8h-5.8c-1 0-1.8-.8-1.8-1.8 0-.6.3-1.1.7-1.5 1.1-.7 1.8-2 1.8-3.4v-1.3c0-1.1-.9-2-2-2h-1.1c-1.5 0-2.7-1.1-2.7-2.6 0-.9.5-1.7 1.2-2.2 0 0 3-2.2 5.4-4.6.4-.4.9-.6 1.4-.6Zm0 2c-1.2 1.2-2.5 2.2-3.6 3.1-.9.7-1.7 1.3-2.2 1.6v.1c0 0 0 .1.1.1h1.1c2.2 0 4 1.8 4 4v1.3c0 .4-.1.7-.2 1.1 2.3-.3 4.2-2.2 4.2-4.6v-1.3c0-2.2 1.8-4 4-4h1.1c.1 0 .2-.1.1-.1v-.1c-.5-.4-1.3-.9-2.2-1.6-1.1-.9-2.4-1.9-3.6-3.1h-.8Z" />
                            </svg>
                        </div>
                        <h1 className="text-2xl font-bold text-indigo-300">{t.title}</h1>

                        <div className="ml-auto flex items-center gap-3">
                            <select
                                value={selectedLanguage}
                                onChange={(e) => setSelectedLanguage(e.target.value)}
                                className="border border-indigo-800 px-3 py-1 rounded-lg text-sm bg-indigo-900 text-gray-200"
                            >
                                <option value="english">English</option>
                                <option value="telugu">తెలుగు</option>
                            </select>

                            <button
                                onClick={startNewChat}
                                className="flex items-center gap-1 text-sm bg-indigo-800 hover:bg-indigo-700 text-gray-200 py-1 px-3 rounded-lg transition-colors"
                            >
                                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                    <path d="M12 5v14M5 12h14" />
                                </svg>
                                {t.newChat}
                            </button>
                        </div>
                    </div>

                    {/* Messages container */}
                    <div className="flex-1 overflow-y-auto mb-4 space-y-4 p-2 text-gray-200">
                        {messages.length === 0 ? (
                            <div className="flex items-center justify-center h-full text-gray-400">
                                <div className="text-center">
                                    <div className="text-4xl mb-2">🌾</div>
                                    <p>{t.welcomeMessage}</p>
                                </div>
                            </div>
                        ) : (
                            messages.map(message => (
                                <div
                                    key={message.id}
                                    className={`flex ${message.isUser ? 'justify-end' : 'justify-start'}`}
                                >
                                    <div
                                        className={`max-w-[75%] rounded-lg p-3 ${message.isUser
                                            ? 'bg-indigo-700 text-gray-100 rounded-br-none'
                                            : 'bg-gray-800 text-gray-100 rounded-bl-none'
                                            }`}
                                    >
                                        <p className="whitespace-pre-wrap">{message.content}</p>

                                        {message.transcribedText && message.transcribedText !== t.voiceMessage && (
                                            <div className="mt-1 text-sm text-indigo-300 italic">
                                                "{message.transcribedText}"
                                            </div>
                                        )}

                                        {message.audioUrl && (
                                            <AudioPlayer audioUrl={message.audioUrl} />
                                        )}
                                    </div>
                                </div>
                            ))
                        )}
                        {loading && (
                            <div className="flex justify-start">
                                <div className="bg-gray-800 text-gray-200 rounded-lg rounded-bl-none p-3">
                                    <div className="flex space-x-1">
                                        <div className="w-2 h-2 bg-indigo-400 rounded-full animate-bounce"></div>
                                        <div className="w-2 h-2 bg-indigo-400 rounded-full animate-bounce" style={{ animationDelay: '0.2s' }}></div>
                                        <div className="w-2 h-2 bg-indigo-400 rounded-full animate-bounce" style={{ animationDelay: '0.4s' }}></div>
                                    </div>
                                </div>
                            </div>
                        )}
                        <div ref={messagesEndRef} />
                    </div>

                    {/* Input area */}
                    <div className="mt-auto border-t border-indigo-900 pt-4">
                        <div className="flex gap-2 items-end">
                            <button
                                onClick={handleRecord}
                                className={`p-3 rounded-full ${isRecording ? 'bg-red-600 hover:bg-red-700' : 'bg-indigo-600 hover:bg-indigo-700'} text-white flex-shrink-0 transition-colors`}
                                title={isRecording ? "Stop recording" : "Record voice message"}
                            >
                                {isRecording ? (
                                    <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                        <rect x="6" y="6" width="12" height="12" rx="1"></rect>
                                    </svg>
                                ) : (
                                    <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                        <path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"></path>
                                        <path d="M19 10v2a7 7 0 0 1-14 0v-2"></path>
                                        <line x1="12" y1="19" x2="12" y2="23"></line>
                                        <line x1="8" y1="23" x2="16" y2="23"></line>
                                    </svg>
                                )}
                            </button>
                            <div className="relative flex-1">
                                <textarea
                                    rows="1"
                                    value={query}
                                    onChange={(e) => setQuery(e.target.value)}
                                    onKeyPress={handleKeyPress}
                                    placeholder={t.inputPlaceholder}
                                    className="border border-indigo-800 w-full px-4 py-3 rounded-lg text-base resize-none bg-indigo-900 text-gray-200 focus:outline-none focus:border-indigo-500"
                                    style={{ minHeight: '48px', maxHeight: '120px' }}
                                />
                            </div>
                            <button
                                onClick={handleTextQuery}
                                disabled={!query.trim() || loading}
                                className={`p-3 rounded-full ${(!query.trim() || loading) ? 'bg-gray-600 cursor-not-allowed' : 'bg-indigo-600 hover:bg-indigo-700'} text-white flex-shrink-0 transition-colors`}
                            >
                                <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                    <line x1="22" y1="2" x2="11" y2="13"></line>
                                    <polygon points="22 2 15 22 11 13 2 9 22 2"></polygon>
                                </svg>
                            </button>
                        </div>

                        {/* Hidden audio element for playback */}
                        <audio ref={audioRef} className="hidden" />
                    </div>
                </div>
            </div>
        </div>
    );
}

export default AgroAI;