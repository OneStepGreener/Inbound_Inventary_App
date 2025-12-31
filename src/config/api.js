/**
 * API Configuration
 * Base URL for backend API calls
 * 
 * For Android Emulator: Use 'http://10.0.2.2:5000'
 * For iOS Simulator: Use 'http://localhost:5000'
 * For Physical Device: Use your computer's IP address (e.g., 'http://192.168.1.100:5000')
 * 
 * To find your IP on Windows: ipconfig (look for IPv4 Address)
 * To find your IP on Mac/Linux: ifconfig or ip addr
 */

// Change this to match your setup
// For development, you can use Platform.OS to auto-detect
import { Platform } from 'react-native';

// Default configuration
// NOTE: All URLs must include the base path prefix: /aiml/corporatewebsite
const API_CONFIG = {
  // For Android Emulator
  // ANDROID_EMULATOR: 'http://10.0.2.2:5000/aiml/corporatewebsite',
  // For iOS Simulator
  // IOS_SIMULATOR: 'http://localhost:5000/aiml/corporatewebsite',
  // For Physical Device - UPDATE THIS WITH YOUR COMPUTER'S IP ADDRESS
  PHYSICAL_DEVICE: 'http://192.168.5.8:5000/aiml/corporatewebsite', // Change this IP to your computer's IP
};

// Auto-detect based on platform (for emulator/simulator)
// For physical devices, you'll need to manually set the IP
const getBaseURL = () => {
  // If emulator URLs are commented out, always use physical device URL
  if (Platform.OS === 'android' && API_CONFIG.ANDROID_EMULATOR) {
    return API_CONFIG.ANDROID_EMULATOR;
  } else if (Platform.OS === 'ios' && API_CONFIG.IOS_SIMULATOR) {
    return API_CONFIG.IOS_SIMULATOR;
  }
  // Default to physical device (or fallback if emulator URLs not set)
  return API_CONFIG.PHYSICAL_DEVICE;
};

export const BASE_URL = getBaseURL();

// Debug: Log the base URL being used (remove in production)
if (__DEV__) {
  console.log('🔍 [API Config] Base URL:', BASE_URL);
  console.log('🔍 [API Config] Platform:', Platform.OS);
}

// API Endpoints
export const API_ENDPOINTS = {
  SCAN_BARCODE: '/barcode/scan',
  SCAN_AND_START_CYCLE: '/barcode/cycle/scan-and-start',
  SCAN_AND_RECORD_INBOUND_WEIGHT: '/barcode/inbound/scan-weight',
  UPDATE_CYCLE_STATUS: '/barcode/cycle',
  GET_CYCLE_DETAILS: '/barcode/cycle',
};

export default {
  BASE_URL,
  API_ENDPOINTS,
};
