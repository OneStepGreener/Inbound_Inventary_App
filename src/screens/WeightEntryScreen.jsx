/**
 * WeightEntryScreen Component
 * Screen shown after a successful barcode scan where user can enter weight
 * White & blue theme to match the rest of the app
 * Integrates with backend API to record inbound weight
 *
 * @param {string} barcode - The scanned barcode value
 * @param {Function} onSubmit - Callback when weight is submitted (receives { barcode, weight, success, data })
 * @param {Function} onBack - Callback to go back to scanner without submitting
 */

import React, { useState, useEffect } from 'react';
import {
  View,
  Text,
  StyleSheet,
  TextInput,
  TouchableOpacity,
  Keyboard,
  StatusBar,
  Platform,
  ActivityIndicator,
  Alert,
} from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';
import { BASE_URL, API_ENDPOINTS } from '../config/api';
import { testAPIConnection } from '../utils/apiTest';

const WeightEntryScreen = ({ barcode, onSubmit, onBack }) => {
  const insets = useSafeAreaInsets();
  const [weight, setWeight] = useState('');
  const [isFocused, setIsFocused] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState('');

  // Test API connection on component mount (in development)
  useEffect(() => {
    if (__DEV__) {
      console.log('🔍 [WeightEntry] Component mounted, testing API connection...');
      testAPIConnection().then(result => {
        if (result.success) {
          console.log('✅ [WeightEntry] API connection test passed');
        } else {
          console.error('❌ [WeightEntry] API connection test failed:', result.error);
        }
      });
    }
  }, []);

  const handleSubmit = async () => {
    const trimmed = weight.trim();
    if (!trimmed) {
      return;
    }

    // Validate weight is a positive number
    const weightNum = parseFloat(trimmed);
    if (isNaN(weightNum) || weightNum <= 0) {
      setErrorMessage('Please enter a valid positive weight');
      return;
    }

    // Extract barcode_id - handle both string and object formats
    let barcodeId = barcode;
    if (typeof barcode === 'object' && barcode !== null) {
      barcodeId = barcode.barcode_id || barcode.id || barcode;
    }
    if (!barcodeId) {
      setErrorMessage('Invalid barcode. Please scan again.');
      return;
    }

    Keyboard.dismiss();
    setErrorMessage('');
    setIsLoading(true);

    // Validate BASE_URL is set
    if (!BASE_URL) {
      const errorMsg = 'API Base URL is not configured. Check src/config/api.js';
      console.error('❌ [WeightEntry]', errorMsg);
      setErrorMessage(errorMsg);
      setIsLoading(false);
      Alert.alert('Configuration Error', errorMsg, [{ text: 'OK' }]);
      return;
    }

    // Build the full URL
    const fullUrl = `${BASE_URL}${API_ENDPOINTS.SCAN_AND_RECORD_INBOUND_WEIGHT}`;
    
    // Validate full URL
    if (!fullUrl || fullUrl.includes('undefined')) {
      const errorMsg = `Invalid API URL: ${fullUrl}. Check configuration.`;
      console.error('❌ [WeightEntry]', errorMsg);
      setErrorMessage(errorMsg);
      setIsLoading(false);
      Alert.alert('Configuration Error', errorMsg, [{ text: 'OK' }]);
      return;
    }
    
    // Debug logging
    console.log('🔍 [WeightEntry] ========== API REQUEST START ==========');
    console.log('🔍 [WeightEntry] Making API request...');
    console.log('🔍 [WeightEntry] Full URL:', fullUrl);
    console.log('🔍 [WeightEntry] Base URL:', BASE_URL);
    console.log('🔍 [WeightEntry] Endpoint:', API_ENDPOINTS.SCAN_AND_RECORD_INBOUND_WEIGHT);
    console.log('🔍 [WeightEntry] Request Data:', { barcode_id: barcodeId, inbound_weight: weightNum });
    console.log('🔍 [WeightEntry] Request Method: POST');
    console.log('🔍 [WeightEntry] =========================================');

    try {
      // Create AbortController for timeout
      const controller = new AbortController();
      const timeoutId = setTimeout(() => {
        console.log('⏱️ [WeightEntry] Request timeout after 30 seconds');
        controller.abort();
      }, 30000); // 30 second timeout

      console.log('🔍 [WeightEntry] Fetching...', fullUrl);
      const fetchStartTime = Date.now();

      const response = await fetch(
        fullUrl,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
          },
          body: JSON.stringify({
            barcode_id: barcodeId,
            inbound_weight: weightNum,
          }),
          signal: controller.signal,
        }
      );

      clearTimeout(timeoutId);
      const fetchDuration = Date.now() - fetchStartTime;
      console.log(`✅ [WeightEntry] Fetch completed in ${fetchDuration}ms`);

      console.log('🔍 [WeightEntry] Response Status:', response.status);
      console.log('🔍 [WeightEntry] Response OK:', response.ok);
      console.log('🔍 [WeightEntry] Response Headers:', Object.fromEntries(response.headers.entries()));

      // Get response text first to handle non-JSON responses
      const responseText = await response.text();
      console.log('🔍 [WeightEntry] Response Text (first 500 chars):', responseText.substring(0, 500));

      let data;
      try {
        // Try to parse as JSON
        data = JSON.parse(responseText);
        console.log('🔍 [WeightEntry] Parsed JSON Data:', data);
      } catch (jsonError) {
        // If not JSON, handle as error
        console.error('❌ [WeightEntry] JSON Parse Error:', jsonError);
        console.error('❌ [WeightEntry] Response was not JSON:', responseText);
        setIsLoading(false);
        
        // Check if it's an HTML error page
        if (responseText.includes('<!DOCTYPE') || responseText.includes('<html')) {
          setErrorMessage('Server returned HTML instead of JSON. Check backend logs.');
          Alert.alert(
            'Server Error',
            'Backend returned an HTML error page. This usually means:\n\n1. Backend server is not running\n2. Wrong URL/endpoint\n3. Server error occurred\n\nCheck backend console for details.',
            [{ text: 'OK' }]
          );
        } else if (responseText.trim() === '') {
          setErrorMessage('Empty response from server');
          Alert.alert('Error', 'Server returned empty response. Check backend connection.', [{ text: 'OK' }]);
        } else {
          setErrorMessage(`Invalid response format: ${responseText.substring(0, 100)}`);
          Alert.alert('Error', `Server response is not valid JSON:\n\n${responseText.substring(0, 200)}`, [{ text: 'OK' }]);
        }
        return;
      }

      setIsLoading(false);

      if (response.ok && data.status === 'success') {
        // Success - show success message and call onSubmit callback
        Alert.alert(
          'Success',
          `Inbound weight of ${weightNum} kg recorded successfully!`,
          [
            {
              text: 'OK',
              onPress: () => {
                if (onSubmit) {
                  onSubmit({
                    barcode,
                    weight: trimmed,
                    success: true,
                    data: data.data,
                  });
                }
              },
            },
          ]
        );
      } else {
        // Error from API
        const errorMsg =
          data.message || data.error || 'Failed to record inbound weight';
        setErrorMessage(errorMsg);
        Alert.alert('Error', errorMsg, [{ text: 'OK' }]);
      }
    } catch (error) {
      setIsLoading(false);
      console.error('❌ [WeightEntry] Fetch Error:', error);
      console.error('❌ [WeightEntry] Error Type:', error.constructor.name);
      console.error('❌ [WeightEntry] Error Message:', error.message);
      console.error('❌ [WeightEntry] Error Stack:', error.stack);
      
      let errorMsg = error.message || 'Network error. Please check your connection.';
      let alertTitle = 'Connection Error';
      let alertMessage = `Unable to connect to server. Please check:\n\n1. Backend server is running\n2. Correct IP address in src/config/api.js\n3. Device and computer are on same network`;
      
      // Handle specific error types
      if (error.message && error.message.includes('JSON')) {
        errorMsg = 'JSON parsing error. Server response was not valid JSON.';
        alertTitle = 'JSON Parse Error';
        alertMessage = 'The server response could not be parsed as JSON. This might indicate:\n\n1. Backend returned HTML error page\n2. Backend returned empty response\n3. Backend server error\n\nCheck backend logs for details.';
      } else if (error.message && error.message.includes('Network')) {
        errorMsg = 'Network request failed. Check connection.';
        alertTitle = 'Network Error';
        alertMessage = `Network request failed:\n\n${error.message}\n\nPlease check:\n1. Backend server is running\n2. Correct URL: ${fullUrl}\n3. Device and computer on same network\n4. Firewall allows port 5000`;
      } else if (error.name === 'AbortError' || error.message && error.message.includes('timeout')) {
        errorMsg = 'Request timeout. Server took too long to respond.';
        alertTitle = 'Timeout Error';
        alertMessage = 'The server did not respond in time (30 seconds). Check if backend is running and responsive.';
      } else if (error.message && error.message.includes('Failed to fetch') || error.message && error.message.includes('NetworkError')) {
        errorMsg = 'Network request failed. Cannot reach server.';
        alertTitle = 'Network Error';
        alertMessage = `Cannot connect to server:\n\n${fullUrl}\n\nPlease check:\n1. Backend server is running\n2. Correct IP address: 192.168.5.8\n3. Device and computer on same Wi-Fi\n4. Firewall allows port 5000`;
      }
      
      setErrorMessage(errorMsg);
      Alert.alert(alertTitle, alertMessage, [{ text: 'OK' }]);
    }
  };

  return (
    <View style={styles.container}>
      <StatusBar
        barStyle="light-content"
        backgroundColor="#2196F3"
        translucent={Platform.OS === 'android'}
      />

      {/* Header */}
      <View style={[styles.header, { paddingTop: insets.top + 10 }]}> 
        <TouchableOpacity
          style={styles.backButton}
          onPress={onBack}
          activeOpacity={0.7}
        >
          <Text style={styles.backButtonText}>← Back</Text>
        </TouchableOpacity>
        <View style={styles.titleContainer}>
          <Text style={styles.title}>Enter Inbound Weight</Text>
          <Text style={styles.subtitle}>
            Barcode:{' '}
            {typeof barcode === 'object' && barcode !== null
              ? barcode.barcode_id || barcode.id || JSON.stringify(barcode)
              : barcode}
          </Text>
        </View>
      </View>

      {/* Content */}
      <View style={[styles.content, { paddingBottom: insets.bottom + 20 }]}> 
        <View style={styles.card}>
          <Text style={styles.label}>Inbound Weight (kg)</Text>
          <TextInput
            style={[
              styles.input,
              isFocused && styles.inputFocused,
              errorMessage && styles.inputError,
            ]}
            value={weight}
            onChangeText={(text) => {
              setWeight(text);
              setErrorMessage(''); // Clear error when user types
            }}
            onFocus={() => setIsFocused(true)}
            onBlur={() => setIsFocused(false)}
            placeholder="Enter weight in kg"
            placeholderTextColor="#999"
            keyboardType="decimal-pad"
            returnKeyType="done"
            onSubmitEditing={handleSubmit}
            editable={!isLoading}
          />

          {errorMessage ? (
            <Text style={styles.errorText}>{errorMessage}</Text>
          ) : null}

          <TouchableOpacity
            style={[
              styles.submitButton,
              (!weight.trim() || isLoading) && styles.submitButtonDisabled,
            ]}
            onPress={() => {
              console.log('🔍 [WeightEntry] Submit button pressed');
              console.log('🔍 [WeightEntry] Current BASE_URL:', BASE_URL);
              console.log('🔍 [WeightEntry] Current weight:', weight);
              handleSubmit();
            }}
            activeOpacity={0.8}
            disabled={!weight.trim() || isLoading}
          >
            {isLoading ? (
              <View style={styles.loadingContainer}>
                <ActivityIndicator size="small" color="#FFFFFF" />
                <Text style={[styles.submitButtonText, { marginLeft: 10 }]}>
                  Submitting...
                </Text>
              </View>
            ) : (
              <Text style={styles.submitButtonText}>Submit Weight</Text>
            )}
          </TouchableOpacity>

          {/* Debug: Show current API URL */}
          {__DEV__ && (
            <Text style={styles.debugText}>
              API: {BASE_URL || 'NOT CONFIGURED'}
            </Text>
          )}

          <Text style={styles.infoText}>
            This will update the inbound weight and set status to 'inbound'
          </Text>
        </View>
      </View>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#FFFFFF',
  },
  header: {
    backgroundColor: '#2196F3',
    paddingHorizontal: 20,
    paddingBottom: 20,
    position: 'absolute',
    top: 0,
    left: 0,
    right: 0,
    zIndex: 10,
    shadowColor: '#000',
    shadowOffset: {
      width: 0,
      height: 2,
    },
    shadowOpacity: 0.25,
    shadowRadius: 3.84,
    elevation: 5,
  },
  backButton: {
    position: 'absolute',
    top: 0,
    left: 20,
    zIndex: 11,
  },
  titleContainer: {
    alignItems: 'center',
    marginTop: 10,
  },
  backButtonText: {
    color: '#FFFFFF',
    fontSize: 16,
    fontWeight: '600',
  },
  title: {
    fontSize: 28,
    fontWeight: 'bold',
    color: '#FFFFFF',
    marginBottom: 4,
    textAlign: 'center',
  },
  subtitle: {
    fontSize: 14,
    color: '#E3F2FD',
    textAlign: 'center',
  },
  content: {
    flex: 1,
    marginTop: 120,
    paddingHorizontal: 20,
    backgroundColor: '#F5F5F5',
  },
  card: {
    marginTop: 40,
    backgroundColor: '#FFFFFF',
    borderRadius: 16,
    padding: 20,
    shadowColor: '#000',
    shadowOffset: {
      width: 0,
      height: 2,
    },
    shadowOpacity: 0.1,
    shadowRadius: 3.84,
    elevation: 4,
  },
  label: {
    fontSize: 16,
    fontWeight: '600',
    color: '#333',
    marginBottom: 10,
  },
  input: {
    backgroundColor: '#F5F5F5',
    borderRadius: 12,
    paddingHorizontal: 16,
    paddingVertical: 12,
    fontSize: 16,
    color: '#333',
    borderWidth: 2,
    borderColor: '#E0E0E0',
    marginBottom: 20,
  },
  inputFocused: {
    borderColor: '#2196F3',
    backgroundColor: '#FFFFFF',
  },
  inputError: {
    borderColor: '#F44336',
    backgroundColor: '#FFEBEE',
  },
  errorText: {
    color: '#F44336',
    fontSize: 14,
    marginTop: -10,
    marginBottom: 10,
    paddingHorizontal: 4,
  },
  infoText: {
    color: '#666',
    fontSize: 12,
    marginTop: 12,
    textAlign: 'center',
    fontStyle: 'italic',
  },
  loadingContainer: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
  },
  submitButton: {
    backgroundColor: '#2196F3',
    borderRadius: 25,
    paddingVertical: 14,
    alignItems: 'center',
    shadowColor: '#2196F3',
    shadowOffset: {
      width: 0,
      height: 4,
    },
    shadowOpacity: 0.3,
    shadowRadius: 4.65,
    elevation: 8,
  },
  submitButtonDisabled: {
    backgroundColor: '#CCCCCC',
    shadowOpacity: 0,
    elevation: 0,
  },
  submitButtonText: {
    color: '#FFFFFF',
    fontSize: 16,
    fontWeight: '600',
  },
});

export default WeightEntryScreen;
