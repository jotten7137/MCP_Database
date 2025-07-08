document.addEventListener('DOMContentLoaded', function() {
    // DOM elements
    const messageInput = document.getElementById('messageInput');
    const sendMessageBtn = document.getElementById('sendMessage');
    const chatMessages = document.getElementById('chatMessages');
    const clearChatBtn = document.getElementById('clearChat');
    const testConnectionBtn = document.getElementById('testConnection');
    const connectionStatus = document.getElementById('connectionStatus');
    const serverUrlInput = document.getElementById('serverUrl');
    const apiKeyInput = document.getElementById('apiKey');
    const startRecordingBtn = document.getElementById('startRecording');
    const stopRecordingBtn = document.getElementById('stopRecording');
    const recordingStatus = document.getElementById('recordingStatus');
    const audioPlayback = document.getElementById('audioPlayback');
    const transcriptionInput = document.getElementById('transcriptionInput');
    const sendTranscriptionBtn = document.getElementById('sendTranscription');
    const enableVoiceCheckbox = document.getElementById('enableVoice');
    const voiceOptions = document.getElementById('voiceOptions');
    const voiceVolume = document.getElementById('voiceVolume');
    const testVoiceBtn = document.getElementById('testVoice');
    const voiceModelSelect = document.getElementById('voiceModel');

    // Add new button for auto-transcription
    const autoTranscribeBtn = document.createElement('button');
    autoTranscribeBtn.id = 'autoTranscribe';
    autoTranscribeBtn.className = 'btn btn-primary ms-2';
    autoTranscribeBtn.innerHTML = '<i class="fas fa-magic"></i> Auto-Transcribe';
    autoTranscribeBtn.disabled = true;
    // Insert after send button
    sendTranscriptionBtn.parentNode.insertBefore(autoTranscribeBtn, sendTranscriptionBtn.nextSibling);
    
    // Add button for real-time speech recognition
    const realTimeSpeechBtn = document.createElement('button');
    realTimeSpeechBtn.id = 'realTimeSpeech';
    realTimeSpeechBtn.className = 'btn btn-info mb-2 w-100';
    realTimeSpeechBtn.innerHTML = '<i class="fas fa-microphone-alt"></i> Start Speech Recognition';
    // Insert at top of manual transcription area
    transcriptionInput.parentNode.insertBefore(realTimeSpeechBtn, transcriptionInput);

    // State variables
    let sessionId = null;
    let mediaRecorder = null;
    let audioChunks = [];
    let audioBlob = null;
    let recognition = null;
    let isListening = false;
    
    // Check browser support for Speech Recognition
    const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
    const hasSpeechRecognition = !!SpeechRecognition;
    
    if (!hasSpeechRecognition) {
        realTimeSpeechBtn.disabled = true;
        realTimeSpeechBtn.title = 'Speech recognition not supported in this browser';
        realTimeSpeechBtn.innerHTML += ' (Not supported)';
    }
    
    // Load saved settings
    loadSettings();
    
    // Event listeners
    sendMessageBtn.addEventListener('click', sendMessage);
    messageInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            sendMessage();
        }
    });
    clearChatBtn.addEventListener('click', clearChat);
    testConnectionBtn.addEventListener('click', testConnection);
    startRecordingBtn.addEventListener('click', startRecording);
    stopRecordingBtn.addEventListener('click', stopRecording);
    sendTranscriptionBtn.addEventListener('click', sendTranscription);
    autoTranscribeBtn.addEventListener('click', performAutoTranscription);
    realTimeSpeechBtn.addEventListener('click', toggleSpeechRecognition);
    
    // Save settings when changed
    serverUrlInput.addEventListener('change', saveSettings);
    apiKeyInput.addEventListener('change', saveSettings);
    
    // Initialize speech recognition if supported
    if (hasSpeechRecognition) {
        initSpeechRecognition();
    }
    
    // Functions
    function loadSettings() {
        const savedServerUrl = localStorage.getItem('mcpServerUrl');
        const savedApiKey = localStorage.getItem('mcpApiKey');
        
        if (savedServerUrl) serverUrlInput.value = savedServerUrl;
        if (savedApiKey) apiKeyInput.value = savedApiKey;
    }
    
    function saveSettings() {
        localStorage.setItem('mcpServerUrl', serverUrlInput.value);
        localStorage.setItem('mcpApiKey', apiKeyInput.value);
    }
    
    function initSpeechRecognition() {
        recognition = new SpeechRecognition();
        recognition.continuous = true;
        recognition.interimResults = true;
        recognition.lang = 'en-US'; // Set language
        
        recognition.onstart = function() {
            isListening = true;
            realTimeSpeechBtn.innerHTML = '<i class="fas fa-stop-circle"></i> Stop Speech Recognition';
            realTimeSpeechBtn.classList.remove('btn-info');
            realTimeSpeechBtn.classList.add('btn-danger');
            transcriptionInput.placeholder = 'Listening...';
        };
        
        recognition.onend = function() {
            isListening = false;
            realTimeSpeechBtn.innerHTML = '<i class="fas fa-microphone-alt"></i> Start Speech Recognition';
            realTimeSpeechBtn.classList.remove('btn-danger');
            realTimeSpeechBtn.classList.add('btn-info');
            transcriptionInput.placeholder = 'Type what you hear...';
        };
        
        recognition.onresult = function(event) {
            let interimTranscript = '';
            let finalTranscript = '';
            
            for (let i = event.resultIndex; i < event.results.length; ++i) {
                if (event.results[i].isFinal) {
                    finalTranscript += event.results[i][0].transcript;
                } else {
                    interimTranscript += event.results[i][0].transcript;
                }
            }
            
            // Update the transcription input with final + interim results
            transcriptionInput.value = finalTranscript + interimTranscript;
        };
        
        recognition.onerror = function(event) {
            console.error('Speech recognition error', event.error);
            if (isListening) {
                toggleSpeechRecognition(); // Stop recognition
            }
            transcriptionInput.placeholder = 'Speech recognition error: ' + event.error;
        };
    }
    
    function toggleSpeechRecognition() {
        if (!hasSpeechRecognition) return;
        
        if (isListening) {
            recognition.stop();
        } else {
            recognition.start();
        }
    }
    
    async function sendMessage() {
        const message = messageInput.value.trim();
        if (!message) return;
        
        // Add user message to chat
        addMessageToChat('user', message);
        messageInput.value = '';
        
        // Show typing indicator
        const typingIndicator = addTypingIndicator();
        
        try {
            const response = await callMcpServer('/api/chat', {
                message: message,
                session_id: sessionId
            });
            
            // Remove typing indicator
            typingIndicator.remove();
            
            // Process response
            if (response) {
                // Save session ID if not already set
                if (!sessionId && response.session_id) {
                    sessionId = response.session_id;
                }
                
                // Add assistant message to chat
                addMessageToChat('assistant', response.message);
                
                // Handle tool calls if present (these would be future tool calls that weren't processed yet)
                if (response.tool_calls && response.tool_calls.length > 0) {
                    console.log('Tool calls detected:', response.tool_calls);
                    
                    // Add tool call indicator to the chat
                    for (const toolCall of response.tool_calls) {
                        addToolCallToChat(toolCall);
                    }
                }
                
                // Handle audio response if available
                if (response.audio_response_id) {
                    playAudioResponse(response.audio_response_id);
                }
            }
        } catch (error) {
            // Remove typing indicator
            typingIndicator.remove();
            
            // Display error message
            addErrorMessage(`Error: ${error.message}`);
        }
    }
    
    async function sendTranscription() {
        const transcription = transcriptionInput.value.trim();
        if (!transcription) return;
        
        // Add user message to chat
        addMessageToChat('user', transcription);
        transcriptionInput.value = '';
        
        // Show typing indicator
        const typingIndicator = addTypingIndicator();
        
        try {
            const response = await callMcpServer('/api/chat', {
                message: transcription,
                session_id: sessionId
            });
            
            // Remove typing indicator
            typingIndicator.remove();
            
            // Process response
            if (response) {
                // Save session ID if not already set
                if (!sessionId && response.session_id) {
                    sessionId = response.session_id;
                }
                
                // Add assistant message to chat
                addMessageToChat('assistant', response.message);
                
                // Handle audio response if available
                if (response.audio_response_id) {
                    playAudioResponse(response.audio_response_id);
                }
            }
        } catch (error) {
            // Remove typing indicator
            typingIndicator.remove();
            
            // Display error message
            addErrorMessage(`Error: ${error.message}`);
        }
    }
    
    function addMessageToChat(role, message) {
        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${role}-message`;
        
        // Convert markdown to HTML if the message contains markdown
        const messageHtml = marked.parse(message);
        
        messageDiv.innerHTML = `
            <div class="content">
                <div class="message-text">${messageHtml}</div>
                <div class="message-time">${new Date().toLocaleTimeString()}</div>
            </div>
        `;
        
        chatMessages.appendChild(messageDiv);
        
        // Scroll to bottom
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }
    
    function addToolCallToChat(toolCall) {
        const toolDiv = document.createElement('div');
        toolDiv.className = 'tool-call';
        toolDiv.style.cssText = `
            background: #f8f9fa;
            border-left: 4px solid #007bff;
            padding: 10px;
            margin: 10px 0;
            border-radius: 4px;
            font-family: monospace;
            font-size: 0.9em;
        `;
        
        toolDiv.innerHTML = `
            <div style="color: #007bff; font-weight: bold; margin-bottom: 5px;">
                🔧 Tool Call: ${toolCall.tool_name}
            </div>
            <div style="color: #666;">
                Parameters: ${JSON.stringify(toolCall.parameters, null, 2)}
            </div>
        `;
        
        chatMessages.appendChild(toolDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }
    
    function addTypingIndicator() {
        const indicatorDiv = document.createElement('div');
        indicatorDiv.className = 'typing-indicator';
        indicatorDiv.innerHTML = `
            <div class="typing-dot"></div>
            <div class="typing-dot"></div>
            <div class="typing-dot"></div>
        `;
        
        chatMessages.appendChild(indicatorDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;
        
        return indicatorDiv;
    }
    
    function addErrorMessage(error) {
        const errorDiv = document.createElement('div');
        errorDiv.className = 'alert alert-danger mt-3';
        errorDiv.textContent = error;
        
        chatMessages.appendChild(errorDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;
        
        // Remove error after 5 seconds
        setTimeout(() => {
            errorDiv.remove();
        }, 5000);
    }
    
    async function callMcpServer(endpoint, data) {
        const serverUrl = serverUrlInput.value;
        const apiKey = apiKeyInput.value;
        
        const response = await fetch(`${serverUrl}${endpoint}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-API-Key': apiKey
            },
            body: JSON.stringify(data)
        });
        
        if (!response.ok) {
            throw new Error(`${response.status} - ${await response.text()}`);
        }
        
        return await response.json();
    }
    
    // Function to play audio responses
    // Completely rewritten audio playback function
    function playAudioResponse(audioId) {
        try {
            console.log("Playing audio response:", audioId);
            
            // Create a self-contained function that doesn't rely on external variables
            (function(id) {
                // Get the DOM elements we need
                var serverUrlInput = document.getElementById('serverUrl');
                var apiKeyInput = document.getElementById('apiKey');
                var voiceVolumeInput = document.getElementById('voiceVolume');
                
                // Get the server URL and API key
                var serverUrl = serverUrlInput ? serverUrlInput.value : 'http://localhost:8000';
                var apiKey = apiKeyInput ? apiKeyInput.value : '';
                
                // Find the message container
                var lastMessageSelector = '.assistant-message:last-child .content';
                var messageContainer = document.querySelector(lastMessageSelector);
                
                if (!messageContainer) {
                    console.error("Could not find message container:", lastMessageSelector);
                    return;
                }
                
                // Create a loading indicator
                var loadingDiv = document.createElement('div');
                loadingDiv.textContent = 'Loading audio...';
                loadingDiv.style.fontSize = 'small';
                loadingDiv.style.color = '#666';
                loadingDiv.style.margin = '10px 0';
                messageContainer.appendChild(loadingDiv);
                
                // Build the audio URL
                var audioUrl = serverUrl + '/api/audio/' + id + '?t=' + Date.now();
                
                // Fetch the audio
                fetch(audioUrl, {
                    headers: {
                        'X-API-Key': apiKey,
                        'Cache-Control': 'no-cache'
                    }
                })
                .then(function(response) {
                    if (!response.ok) {
                        throw new Error('HTTP error ' + response.status);
                    }
                    return response.blob();
                })
                .then(function(blob) {
                    console.log("Audio blob received:", blob.type, blob.size + " bytes");
                    
                    // Create an object URL
                    var objectUrl = URL.createObjectURL(blob);
                    
                    // Create the audio element
                    var newAudio = document.createElement('audio');
                    newAudio.controls = true;
                    newAudio.style.width = '100%';
                    newAudio.style.marginTop = '10px';
                    newAudio.src = objectUrl;

                    newAudio.playbackRate = 1.25;
                    
                    // Set volume if available
                    if (voiceVolumeInput && !isNaN(parseFloat(voiceVolumeInput.value))) {
                        newAudio.volume = parseFloat(voiceVolumeInput.value);
                    }
                    
                    // Remove loading indicator
                    messageContainer.removeChild(loadingDiv);
                    
                    // Add audio to the message
                    messageContainer.appendChild(newAudio);
                    
                    // Add play button for better UX
                    var playBtn = document.createElement('button');
                    playBtn.textContent = 'Play Audio';
                    playBtn.style.marginTop = '5px';
                    playBtn.style.padding = '5px 10px';
                    playBtn.style.background = '#007bff';
                    playBtn.style.color = 'white';
                    playBtn.style.border = 'none';
                    playBtn.style.borderRadius = '3px';
                    playBtn.style.cursor = 'pointer';
                    
                    playBtn.onclick = function() {
                        newAudio.play();
                    };
                    
                    messageContainer.appendChild(playBtn);
                    
                    // Try autoplay with fallback
                    setTimeout(function() {
                        var playPromise = newAudio.play();
                        if (playPromise !== undefined) {
                            playPromise.catch(function(error) {
                                console.log("Autoplay prevented:", error);
                            });
                        }
                    }, 500);
                })
                .catch(function(error) {
                    console.error("Error fetching audio:", error);
                    
                    // Remove loading div
                    if (messageContainer.contains(loadingDiv)) {
                        messageContainer.removeChild(loadingDiv);
                    }
                    
                    // Show error
                    var errorDiv = document.createElement('div');
                    errorDiv.textContent = 'Error playing audio: ' + error.message;
                    errorDiv.style.color = 'red';
                    errorDiv.style.margin = '10px 0';
                    messageContainer.appendChild(errorDiv);
                });
            })(audioId);  // Pass audioId to the self-executing function
            
        } catch (e) {
            // Last resort error handler
            console.error("Fatal error in playAudioResponse:", e);
            alert("Audio playback error: " + e.message);
        }
    }
    
    async function testConnection() {
        const serverUrl = serverUrlInput.value;
        
        connectionStatus.innerHTML = '<span class="text-info">Testing connection...</span>';
        
        try {
            const response = await fetch(serverUrl);
            const data = await response.json();
            
            if (response.ok) {
                connectionStatus.innerHTML = '<span class="text-success">Connected successfully!</span>';
                console.log('Server response:', data);
            } else {
                connectionStatus.innerHTML = `<span class="text-danger">Error: ${response.status}</span>`;
            }
        } catch (error) {
            connectionStatus.innerHTML = `<span class="text-danger">Error: ${error.message}</span>`;
        }
    }
    
    function clearChat() {
        chatMessages.innerHTML = '';
        sessionId = null;
    }
    
    // Voice recording functions
    function startRecording() {
        if (navigator.mediaDevices && navigator.mediaDevices.getUserMedia) {
            navigator.mediaDevices.getUserMedia({ audio: true })
                .then(stream => {
                    mediaRecorder = new MediaRecorder(stream);
                    audioChunks = [];
                    
                    mediaRecorder.ondataavailable = event => {
                        audioChunks.push(event.data);
                    };
                    
                    mediaRecorder.onstop = () => {
                        audioBlob = new Blob(audioChunks, { type: 'audio/wav' });
                        const audioUrl = URL.createObjectURL(audioBlob);
                        
                        // Create audio element
                        audioPlayback.innerHTML = '';
                        const audioElement = document.createElement('audio');
                        audioElement.controls = true;
                        audioElement.src = audioUrl;
                        audioElement.className = 'w-100';
                        audioPlayback.appendChild(audioElement);
                        
                        // Enable auto-transcribe button
                        autoTranscribeBtn.disabled = false;
                        
                        // Update status
                        recordingStatus.textContent = 'Recording complete.';
                        recordingStatus.innerHTML += '<div class="mt-2">';
                        recordingStatus.innerHTML += '<span class="badge bg-success me-2">Option 1</span> Click "Auto-Transcribe" to use browser speech recognition<br>';
                        recordingStatus.innerHTML += '<span class="badge bg-info me-2">Option 2</span> Start real-time speech recognition<br>';
                        recordingStatus.innerHTML += '<span class="badge bg-primary me-2">Option 3</span> Type transcription manually';
                        recordingStatus.innerHTML += '</div>';
                    };
                    
                    mediaRecorder.start();
                    startRecordingBtn.disabled = true;
                    stopRecordingBtn.disabled = false;
                    startRecordingBtn.classList.add('recording');
                    recordingStatus.textContent = 'Recording in progress...';
                })
                .catch(error => {
                    console.error('Error accessing microphone:', error);
                    recordingStatus.textContent = 'Error accessing microphone. Please check permissions.';
                });
        } else {
            recordingStatus.textContent = 'Audio recording not supported in this browser.';
        }
    }
    
    function stopRecording() {
        if (mediaRecorder && mediaRecorder.state !== 'inactive') {
            mediaRecorder.stop();
            mediaRecorder.stream.getTracks().forEach(track => track.stop());
            
            startRecordingBtn.disabled = false;
            stopRecordingBtn.disabled = true;
            startRecordingBtn.classList.remove('recording');
        }
    }
    
    function performAutoTranscription() {
        if (!audioBlob) {
            recordingStatus.textContent = 'No recording available. Please record audio first.';
            return;
        }
        
        // Two options for transcription:
        // 1. Use server-side transcription (currently not working)
        // 2. Use browser's speech recognition API (more compatible)
        
        // Let's use the browser's speech recognition
        if (hasSpeechRecognition) {
            recordingStatus.textContent = 'Transcribing using browser speech recognition...';
            
            // Create an audio element to play the recording
            const audioElement = document.createElement('audio');
            audioElement.src = URL.createObjectURL(audioBlob);
            
            // Create a new speech recognition instance for this operation
            const transcriptionRecognition = new SpeechRecognition();
            transcriptionRecognition.continuous = true;
            transcriptionRecognition.lang = 'en-US';
            
            let finalTranscript = '';
            
            transcriptionRecognition.onresult = function(event) {
                for (let i = event.resultIndex; i < event.results.length; ++i) {
                    if (event.results[i].isFinal) {
                        finalTranscript += event.results[i][0].transcript;
                    }
                }
            };
            
            transcriptionRecognition.onend = function() {
                if (finalTranscript) {
                    transcriptionInput.value = finalTranscript;
                    recordingStatus.textContent = 'Transcription complete. You can edit it before sending.';
                } else {
                    recordingStatus.textContent = 'No transcription detected. Please try again or type manually.';
                }
            };
            
            // Start recognition and play the audio
            transcriptionRecognition.start();
            audioElement.play();
            
            // Stop recognition when audio ends
            audioElement.onended = function() {
                setTimeout(() => {
                    transcriptionRecognition.stop();
                }, 500); // Small delay to catch the last bit of speech
            };
        } else {
            // Fall back to server-side transcription (which is currently not working)
            sendAudioForTranscription(audioBlob);
        }
    }
    
    // Server-side transcription (kept for reference, but currently not working)
    async function sendAudioForTranscription(audioBlob) {
        try {
            recordingStatus.textContent = 'Transcribing audio...';
            
            // Convert blob to base64
            const reader = new FileReader();
            reader.readAsDataURL(audioBlob);
            
            reader.onloadend = async function() {
                const base64Audio = reader.result.split(',')[1];
                
                try {
                    const serverUrl = serverUrlInput.value;
                    const apiKey = apiKeyInput.value;
                    
                    const response = await fetch(`${serverUrl}/api/transcribe`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'X-API-Key': apiKey
                        },
                        body: JSON.stringify({
                            audio_data: base64Audio,
                            session_id: sessionId,
                            format: 'wav'
                        })
                    });
                    
                    if (!response.ok) {
                        throw new Error(`${response.status} - ${await response.text()}`);
                    }
                    
                    const data = await response.json();
                    const transcription = data.text || '';
                    
                    if (transcription) {
                        // Populate transcription input with the result
                        transcriptionInput.value = transcription;
                        recordingStatus.textContent = 'Transcription complete! You can edit it before sending.';
                    } else {
                        recordingStatus.textContent = 'No transcription received. Please type what you hear manually.';
                    }
                } catch (error) {
                    console.error('Transcription error:', error);
                    recordingStatus.textContent = `Transcription error: ${error.message}. Please type what you hear manually.`;
                }
            };
        } catch (error) {
            console.error('Error sending audio:', error);
            recordingStatus.textContent = 'Error processing audio. Please try again.';
        }
    }

    // Event listeners for voice settings
    enableVoiceCheckbox.addEventListener('change', function() {
        voiceOptions.style.display = this.checked ? 'block' : 'none';
        saveSettings();
    });

    voiceVolume.addEventListener('change', saveSettings);
    voiceModelSelect.addEventListener('change', saveSettings);

    testVoiceBtn.addEventListener('click', function() {
        // Send a request to test voice
        fetch(`${serverUrlInput.value}/api/chat`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-API-Key': apiKeyInput.value
            },
            body: JSON.stringify({
                message: "This is a test of the voice response feature.",
                session_id: null,
                voice_test: true  // Special flag to indicate this is a test
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.audio_response_id) {
                playAudioResponse(data.audio_response_id);
            }
        })
        .catch(error => {
            addErrorMessage(`Voice test error: ${error.message}`);
        });
    });

    // Update loadSettings
    function loadSettings() {
        const savedServerUrl = localStorage.getItem('mcpServerUrl');
        const savedApiKey = localStorage.getItem('mcpApiKey');
        const savedVoiceEnabled = localStorage.getItem('mcpVoiceEnabled');
        const savedVoiceVolume = localStorage.getItem('mcpVoiceVolume');
        const savedVoiceModel = localStorage.getItem('mcpVoiceModel');
        
        if (savedServerUrl) serverUrlInput.value = savedServerUrl;
        if (savedApiKey) apiKeyInput.value = savedApiKey;
        
        if (savedVoiceEnabled !== null) {
            enableVoiceCheckbox.checked = savedVoiceEnabled === 'true';
            voiceOptions.style.display = enableVoiceCheckbox.checked ? 'block' : 'none';
        }
        
        if (savedVoiceVolume) voiceVolume.value = savedVoiceVolume;
        if (savedVoiceModel) voiceModelSelect.value = savedVoiceModel;
    }

    // Update saveSettings
    function saveSettings() {
        localStorage.setItem('mcpServerUrl', serverUrlInput.value);
        localStorage.setItem('mcpApiKey', apiKeyInput.value);
        localStorage.setItem('mcpVoiceEnabled', enableVoiceCheckbox.checked);
        localStorage.setItem('mcpVoiceVolume', voiceVolume.value);
        localStorage.setItem('mcpVoiceModel', voiceModelSelect.value);
    }
});