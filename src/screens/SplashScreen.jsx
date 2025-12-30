/**
 * SplashScreen Component
 * Displays the splash screen image with optional auto-navigation
 * Fully responsive for all phone sizes and orientations
 *
 * @param {Function} onFinish - Callback function called when splash screen should finish
 * @param {number} duration - Duration in milliseconds to show splash screen (default: 3000)
 */

import React, { useEffect } from 'react';
import {
  View,
  Image,
  StyleSheet,
  StatusBar,
  useWindowDimensions,
  Platform,
} from 'react-native';

const SplashScreen = ({ onFinish, duration = 3000 }) => {
  const { width, height } = useWindowDimensions();

  useEffect(() => {
    if (onFinish && duration > 0) {
      const timer = setTimeout(() => {
        onFinish();
      }, duration);

      return () => clearTimeout(timer);
    }
  }, [onFinish, duration]);

  // Calculate responsive image dimensions
  const imageStyle = {
    width: width,
    height: height,
  };

  return (
    <View style={[styles.container, { width, height }]}>
      <StatusBar
        barStyle="light-content"
        backgroundColor="#2d5016"
        translucent={Platform.OS === 'android'}
      />
      <Image
        source={require('../assets/img/Splash Screen.png')}
        style={[styles.splashImage, imageStyle]}
        resizeMode="cover"
      />
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#2d5016', // Green background matching the image
    justifyContent: 'center',
    alignItems: 'center',
  },
  splashImage: {
    flex: 1,
    width: '100%',
    height: '100%',
  },
});

export default SplashScreen;
