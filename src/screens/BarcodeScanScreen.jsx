/**
 * BarcodeScanScreen Component
 * Modern barcode scanning screen with white and blue theme
 * Fully responsive with smooth animations
 * Includes camera scanning functionality with safe error handling
 *
 * @param {Function} onScanSuccess - Callback when barcode is successfully scanned
 * @param {Function} onBack - Callback to navigate back
 */

import React, { useState, useEffect, useRef } from 'react';
import {
  View,
  Text,
  StyleSheet,
  Animated,
  Dimensions,
  StatusBar,
  TouchableOpacity,
  TextInput,
  Keyboard,
  Platform,
  ActivityIndicator,
} from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';
import { request, PERMISSIONS, RESULTS } from 'react-native-permissions';

const { width, height } = Dimensions.get('window');

// Safely import camera modules with error handling
let Camera;
let useCameraDevice;
let useCodeScanner;
let cameraModuleAvailable = false;

try {
  const VisionCamera = require('react-native-vision-camera');
  if (VisionCamera && VisionCamera.Camera && VisionCamera.useCameraDevice && VisionCamera.useCodeScanner) {
    Camera = VisionCamera.Camera;
    useCameraDevice = VisionCamera.useCameraDevice;
    useCodeScanner = VisionCamera.useCodeScanner;
    cameraModuleAvailable = true;
  }
} catch (error) {
  console.warn('react-native-vision-camera not available:', error);
  cameraModuleAvailable = false;
}

const BarcodeScanScreen = ({ onScanSuccess, onBack }) => {
  const [barcodeInput, setBarcodeInput] = useState('');
  const [isFocused, setIsFocused] = useState(false);
  const [cameraActive, setCameraActive] = useState(false);
  const [hasPermission, setHasPermission] = useState(null);
  const [showCamera, setShowCamera] = useState(false);
  const [cameraError, setCameraError] = useState(null);
  const insets = useSafeAreaInsets();

  // Only use camera hooks if module is available
  let device = null;
  let codeScanner = null;

  if (cameraModuleAvailable && useCameraDevice && useCodeScanner) {
    try {
      // eslint-disable-next-line react-hooks/rules-of-hooks
      device = useCameraDevice('back');
      
      // eslint-disable-next-line react-hooks/rules-of-hooks
      codeScanner = useCodeScanner({
        codeTypes: ['ean-13', 'ean-8', 'upc-a', 'upc-e', 'code-128', 'code-39', 'qr'],
        onCodeScanned: (codes) => {
          if (codes.length > 0 && cameraActive) {
            const scannedCode = codes[0].value;
            setCameraActive(false);
            handleBarcodeScanned(scannedCode);
          }
        },
      });
    } catch (error) {
      console.error('Camera hook error:', error);
      setCameraError('Camera initialization failed');
      cameraModuleAvailable = false;
    }
  }

  const camera = useRef(null);

  // Animation values
  const fadeAnim = useRef(new Animated.Value(0)).current;
  const slideAnim = useRef(new Animated.Value(50)).current;
  const pulseAnim = useRef(new Animated.Value(1)).current;
  const scanLineAnim = useRef(new Animated.Value(0)).current;

  // Scanning area dimensions (responsive)
  const scanAreaSize = Math.min(width * 0.75, height * 0.4);

  useEffect(() => {
    // Request camera permission on mount
    requestCameraPermission();

    // Fade in animation on mount
    Animated.parallel([
      Animated.timing(fadeAnim, {
        toValue: 1,
        duration: 500,
        useNativeDriver: true,
      }),
      Animated.timing(slideAnim, {
        toValue: 0,
        duration: 500,
        useNativeDriver: true,
      }),
    ]).start();

    // Start scan line animation
    startScanLineAnimation();

    // Pulse animation for scan button
    Animated.loop(
      Animated.sequence([
        Animated.timing(pulseAnim, {
          toValue: 1.05,
          duration: 1000,
          useNativeDriver: true,
        }),
        Animated.timing(pulseAnim, {
          toValue: 1,
          duration: 1000,
          useNativeDriver: true,
        }),
      ]),
    ).start();
  }, []);

  // Enable camera when permission is granted and device is available
  useEffect(() => {
    if (hasPermission === true && cameraModuleAvailable && device && !showCamera) {
      setShowCamera(true);
      setCameraActive(true);
    }
  }, [hasPermission, device]);

  const requestCameraPermission = async () => {
    try {
      const permission = Platform.OS === 'ios' 
        ? PERMISSIONS.IOS.CAMERA 
        : PERMISSIONS.ANDROID.CAMERA;
      
      const result = await request(permission);
      const granted = result === RESULTS.GRANTED;
      setHasPermission(granted);
    } catch (error) {
      console.error('Permission error:', error);
      setHasPermission(false);
      setCameraError('Failed to request camera permission');
    }
  };

  const startScanLineAnimation = () => {
    Animated.loop(
      Animated.sequence([
        Animated.timing(scanLineAnim, {
          toValue: 1,
          duration: 2000,
          useNativeDriver: true,
        }),
        Animated.timing(scanLineAnim, {
          toValue: 0,
          duration: 2000,
          useNativeDriver: true,
        }),
      ]),
    ).start();
  };

  const handleBarcodeScanned = (code) => {
    Keyboard.dismiss();
    
    // Success animation
    Animated.sequence([
      Animated.timing(pulseAnim, {
        toValue: 1.2,
        duration: 200,
        useNativeDriver: true,
      }),
      Animated.timing(pulseAnim, {
        toValue: 1,
        duration: 200,
        useNativeDriver: true,
      }),
    ]).start();

    // Call success callback
    if (onScanSuccess) {
      onScanSuccess(code);
    }

    // Reactivate camera after 2 seconds
    setTimeout(() => {
      setCameraActive(true);
    }, 2000);
  };

  const handleScan = () => {
    if (barcodeInput.trim().length === 0) {
      // Show error animation
      Animated.sequence([
        Animated.timing(slideAnim, {
          toValue: -10,
          duration: 100,
          useNativeDriver: true,
        }),
        Animated.timing(slideAnim, {
          toValue: 0,
          duration: 100,
          useNativeDriver: true,
        }),
      ]).start();
      return;
    }

    handleBarcodeScanned(barcodeInput.trim());
    setBarcodeInput('');
  };

  const toggleCamera = () => {
    if (!cameraModuleAvailable || !device) {
      setCameraError('Camera not available. Please rebuild the app.');
      return;
    }
    if (hasPermission && device) {
      setShowCamera(!showCamera);
      setCameraActive(!showCamera);
    } else {
      requestCameraPermission();
    }
  };

  const scanLineTranslateY = scanLineAnim.interpolate({
    inputRange: [0, 1],
    outputRange: [0, scanAreaSize - 4],
  });

  // Loading state
  if (hasPermission === null) {
    return (
      <View style={[styles.container, { paddingTop: insets.top }]}>
        <StatusBar barStyle="light-content" backgroundColor="#2196F3" translucent={Platform.OS === 'android'} />
        <View style={[styles.header, { paddingTop: insets.top + 10 }]}>
          <TouchableOpacity
            style={styles.backButton}
            onPress={onBack}
            activeOpacity={0.7}
          >
            <Text style={styles.backButtonText}>← Back</Text>
          </TouchableOpacity>
          <Text style={styles.title}>Scan Barcode</Text>
        </View>
        <View style={styles.loadingContainer}>
          <ActivityIndicator size="large" color="#2196F3" />
          <Text style={styles.loadingText}>Requesting camera permission...</Text>
        </View>
      </View>
    );
  }

  const canUseCamera = cameraModuleAvailable && hasPermission && device && showCamera && Camera && codeScanner;

  return (
    <Animated.View
      style={[
        styles.container,
        {
          opacity: fadeAnim,
        },
      ]}
    >
      <StatusBar barStyle="light-content" backgroundColor="#2196F3" translucent={Platform.OS === 'android'} />
      
      {/* Header - Fixed at top with safe area */}
      <View style={[styles.header, { paddingTop: insets.top + 10 }]}>
        <TouchableOpacity
          style={styles.backButton}
          onPress={onBack}
          activeOpacity={0.7}
        >
          <Text style={styles.backButtonText}>← Back</Text>
        </TouchableOpacity>
        <Text style={styles.title}>Scan Barcode</Text>
        <Text style={styles.subtitle}>
          {canUseCamera ? 'Position barcode in frame or enter manually' : 'Enter barcode manually'}
        </Text>
      </View>

      {/* Camera View - Only render if everything is available */}
      {canUseCamera && (
        <View style={styles.cameraContainer}>
          <Camera
            ref={camera}
            style={StyleSheet.absoluteFill}
            device={device}
            isActive={cameraActive}
            codeScanner={codeScanner}
          />
          
          {/* Scanning frame overlay */}
          <View style={styles.scanFrameOverlay}>
            <View style={[styles.scanFrame, { width: scanAreaSize, height: scanAreaSize }]}>
              <View style={[styles.corner, styles.topLeft]} />
              <View style={[styles.corner, styles.topRight]} />
              <View style={[styles.corner, styles.bottomLeft]} />
              <View style={[styles.corner, styles.bottomRight]} />
              
              {/* Animated scan line */}
              <Animated.View
                style={[
                  styles.scanLine,
                  {
                    transform: [{ translateY: scanLineTranslateY }],
                  },
                ]}
              />
            </View>
          </View>
        </View>
      )}

      {/* Placeholder when camera is not available */}
      {!canUseCamera && (
        <View style={styles.placeholderContainer}>
          <View style={[styles.scanFrame, { width: scanAreaSize, height: scanAreaSize }]}>
            <View style={[styles.corner, styles.topLeft]} />
            <View style={[styles.corner, styles.topRight]} />
            <View style={[styles.corner, styles.bottomLeft]} />
            <View style={[styles.corner, styles.bottomRight]} />
            <View style={styles.scanLine} />
            <View style={styles.placeholderContent}>
              <Text style={styles.placeholderIcon}>📷</Text>
              <Text style={styles.placeholderText}>
                {cameraError || !cameraModuleAvailable 
                  ? 'Camera module not linked. Please rebuild the app.' 
                  : !hasPermission 
                    ? 'Camera permission required' 
                    : !device 
                      ? 'Camera device not available' 
                      : 'Tap to enable camera'}
              </Text>
            </View>
          </View>
        </View>
      )}

      {/* Manual Input Section */}
      <Animated.View
        style={[
          styles.inputContainer,
          {
            transform: [{ translateY: slideAnim }],
            paddingBottom: insets.bottom + 20,
          },
        ]}
      >
        <View style={styles.inputSection}>
          <View style={styles.inputHeader}>
            <Text style={styles.inputLabel}>Enter Barcode</Text>
            {cameraModuleAvailable && (
              <TouchableOpacity onPress={toggleCamera} style={styles.cameraToggle}>
                <Text style={styles.cameraToggleText}>
                  {canUseCamera ? '📷 Hide Camera' : '📷 Show Camera'}
                </Text>
              </TouchableOpacity>
            )}
          </View>
          <TextInput
            style={[
              styles.input,
              isFocused && styles.inputFocused,
            ]}
            value={barcodeInput}
            onChangeText={setBarcodeInput}
            onFocus={() => setIsFocused(true)}
            onBlur={() => setIsFocused(false)}
            placeholder="Type barcode here"
            placeholderTextColor="#999"
            autoCapitalize="none"
            autoCorrect={false}
            returnKeyType="done"
            onSubmitEditing={handleScan}
            keyboardType="default"
          />
          
          <Animated.View
            style={[
              styles.scanButton,
              {
                transform: [{ scale: pulseAnim }],
              },
            ]}
          >
            <TouchableOpacity
              style={[
                styles.scanButtonInner,
                barcodeInput.trim().length === 0 && styles.scanButtonDisabled,
              ]}
              onPress={handleScan}
              activeOpacity={0.8}
              disabled={barcodeInput.trim().length === 0}
            >
              <Text style={styles.scanButtonText}>
                {barcodeInput.trim().length > 0 ? '✓ Process Barcode' : 'Enter Barcode'}
              </Text>
            </TouchableOpacity>
          </Animated.View>
        </View>
      </Animated.View>
    </Animated.View>
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
    marginBottom: 10,
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
    marginBottom: 8,
  },
  subtitle: {
    fontSize: 14,
    color: '#E3F2FD',
  },
  cameraContainer: {
    flex: 1,
    marginTop: 0,
    position: 'center',
    // Use black background so any letterboxing around the camera preview isn't white
    backgroundColor: '#000000',
  },
  scanFrameOverlay: {
    position: 'absolute',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    justifyContent: 'center',
    alignItems: 'center',
    zIndex: 1,
  },
  placeholderContainer: {
    flex: 1,
    marginTop: 120,
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: '#F5F5F5',
  },
  overlay: {
    ...StyleSheet.absoluteFillObject,
    justifyContent: 'center',
    alignItems: 'center',
  },
  overlaySection: {
    backgroundColor: 'rgba(0, 0, 0, 0.5)',
  },
  middleSection: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  scanFrame: {
    borderWidth: 2,
    borderColor: '#2196F3',
    borderRadius: 12,
    backgroundColor: 'transparent',
    position: 'center',
    overflow: 'visible',
    justifyContent: 'center',
    alignItems: 'center',
    top:90
  },
  corner: {
    position: 'absolute',
    width: 30,
    height: 30,
    borderColor: '#2196F3',
    borderWidth: 3,
  },
  topLeft: {
    top: -3,
    left: -3,
    borderRightWidth: 0,
    borderBottomWidth: 0,
    borderTopLeftRadius: 12,
  },
  topRight: {
    top: -3,
    right: -3,
    borderLeftWidth: 0,
    borderBottomWidth: 0,
    borderTopRightRadius: 12,
  },
  bottomLeft: {
    bottom: -3,
    left: -3,
    borderRightWidth: 0,
    borderTopWidth: 0,
    borderBottomLeftRadius: 12,
  },
  bottomRight: {
    bottom: -3,
    right: -3,
    borderLeftWidth: 0,
    borderTopWidth: 0,
    borderBottomRightRadius: 12,
  },
  scanLine: {
    position: 'absolute',
    width: '90%',
    height: 2,
    backgroundColor: '#2196F3',
    shadowColor: '#2196F3',
    shadowOffset: {
      width: 0,
      height: 0,
    },
    shadowOpacity: 1,
    shadowRadius: 10,
    elevation: 5,
  },
  placeholderContent: {
    alignItems: 'center',
    justifyContent: 'center',
  },
  placeholderIcon: {
    fontSize: 48,
    marginBottom: 10,
  },
  placeholderText: {
    fontSize: 14,
    color: '#666',
    textAlign: 'center',
    lineHeight: 20,
    paddingHorizontal: 20,
  },
  inputContainer: {
    backgroundColor: '#FFFFFF',
    borderTopLeftRadius: 20,
    borderTopRightRadius: 20,
    paddingHorizontal: 20,
    paddingTop: 20,
    shadowColor: '#000',
    shadowOffset: {
      width: 0,
      height: -2,
    },
    shadowOpacity: 0.1,
    shadowRadius: 3.84,
    elevation: 5,
  },
  inputSection: {
    marginTop: 10,
  },
  inputHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 10,
  },
  inputLabel: {
    fontSize: 16,
    fontWeight: '600',
    color: '#333',
  },
  cameraToggle: {
    padding: 5,
  },
  cameraToggleText: {
    fontSize: 14,
    color: '#2196F3',
    fontWeight: '600',
  },
  input: {
    backgroundColor: '#F5F5F5',
    borderRadius: 12,
    paddingHorizontal: 20,
    paddingVertical: 16,
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
  scanButton: {
    marginBottom: 10,
  },
  scanButtonInner: {
    backgroundColor: '#2196F3',
    borderRadius: 25,
    paddingVertical: 16,
    paddingHorizontal: 24,
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
  scanButtonDisabled: {
    backgroundColor: '#CCCCCC',
    shadowOpacity: 0,
    elevation: 0,
  },
  scanButtonText: {
    color: '#FFFFFF',
    fontSize: 16,
    fontWeight: '600',
  },
  loadingContainer: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: '#FFFFFF',
  },
  loadingText: {
    marginTop: 10,
    fontSize: 16,
    color: '#666',
  },
});

export default BarcodeScanScreen;
