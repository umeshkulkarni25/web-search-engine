const path = require('path');
const Dotenv = require('dotenv-webpack');

module.exports = {
  mode: 'production',
  entry: [
    './index.js',
  ],
  target: 'node',
  module: {
    rules: [{
      test: /\.js?$/,
      use: 'babel-loader',
      exclude: /node_modules/,
    }],
  },
  plugins: [
    new Dotenv({
      path: './.prod.env',
    }),
  ],
  output: {
    path: path.join(__dirname, 'prod'),
    filename: 'index.js',
  },
};
