/**
 * API Connection Test Utility
 * Use this to test API connectivity from React Native
 */

import { BASE_URL, API_ENDPOINTS } from '../config/api';

export const testAPIConnection = async () => {
  console.log('🧪 [API Test] Starting API connection test...');
  console.log('🧪 [API Test] Base URL:', BASE_URL);
  
  if (!BASE_URL) {
    console.error('❌ [API Test] BASE_URL is undefined!');
    return {
      success: false,
      error: 'BASE_URL is not configured',
    };
  }

  if (BASE_URL.includes('undefined')) {
    console.error('❌ [API Test] BASE_URL contains undefined!');
    return {
      success: false,
      error: 'BASE_URL contains undefined value',
    };
  }

  // Test 1: Basic connectivity
  const testUrl = `${BASE_URL}/test/connection`;
  console.log('🧪 [API Test] Test URL:', testUrl);

  try {
    const response = await fetch(testUrl, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
    });

    console.log('🧪 [API Test] Response Status:', response.status);
    console.log('🧪 [API Test] Response OK:', response.ok);

    const responseText = await response.text();
    console.log('🧪 [API Test] Response Text:', responseText.substring(0, 200));

    let data;
    try {
      data = JSON.parse(responseText);
      console.log('🧪 [API Test] Parsed Data:', data);
    } catch (e) {
      console.error('❌ [API Test] JSON Parse Error:', e);
      return {
        success: false,
        error: 'Response is not valid JSON',
        responseText: responseText.substring(0, 200),
      };
    }

    return {
      success: response.ok,
      status: response.status,
      data: data,
    };
  } catch (error) {
    console.error('❌ [API Test] Fetch Error:', error);
    return {
      success: false,
      error: error.message,
      errorType: error.constructor.name,
    };
  }
};

// Test the inbound weight endpoint
export const testInboundWeightAPI = async (barcodeId, weight) => {
  console.log('🧪 [API Test] Testing inbound weight endpoint...');
  
  const testUrl = `${BASE_URL}${API_ENDPOINTS.SCAN_AND_RECORD_INBOUND_WEIGHT}`;
  console.log('🧪 [API Test] Test URL:', testUrl);
  console.log('🧪 [API Test] Test Data:', { barcode_id: barcodeId, inbound_weight: weight });

  try {
    const response = await fetch(testUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
      body: JSON.stringify({
        barcode_id: barcodeId,
        inbound_weight: weight,
      }),
    });

    console.log('🧪 [API Test] Response Status:', response.status);
    const responseText = await response.text();
    console.log('🧪 [API Test] Response Text:', responseText.substring(0, 500));

    let data;
    try {
      data = JSON.parse(responseText);
      return {
        success: response.ok,
        status: response.status,
        data: data,
      };
    } catch (e) {
      return {
        success: false,
        error: 'Response is not valid JSON',
        responseText: responseText.substring(0, 500),
      };
    }
  } catch (error) {
    console.error('❌ [API Test] Fetch Error:', error);
    return {
      success: false,
      error: error.message,
      errorType: error.constructor.name,
    };
  }
};
