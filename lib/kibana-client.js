import axios from 'axios';

/**
 * Kibana client for executing Elasticsearch queries via Kibana's Console Proxy API
 */
export class KibanaClient {
  constructor(config = {}) {
    this.host = config.host || process.env.KIBANA_HOST || 'localhost';
    this.port = config.port || process.env.KIBANA_PORT || '5601';
    this.protocol = config.protocol || process.env.KIBANA_PROTOCOL || 'http';
    // Match poll-elastic.js pattern exactly - read from env or config, trim whitespace
    this.username = config.username || (process.env.KIBANA_USERNAME ? process.env.KIBANA_USERNAME.trim() : undefined);
    this.password = config.password || (process.env.KIBANA_PASSWORD ? process.env.KIBANA_PASSWORD.trim() : undefined);
    this.authToken = config.authToken || (process.env.KIBANA_AUTH_TOKEN ? process.env.KIBANA_AUTH_TOKEN.trim() : undefined);
    
    this.baseUrl = `${this.protocol}://${this.host}:${this.port}`;
  }

  /**
   * Build authentication headers
   */
  getAuthHeaders() {
    const headers = {
      'Content-Type': 'application/json',
      'kbn-xsrf': 'true',
    };

    if (this.authToken) {
      // If token starts with "Bearer ", use as-is, otherwise assume Basic auth
      if (this.authToken.startsWith('Bearer ')) {
        headers['Authorization'] = this.authToken;
      } else {
        headers['Authorization'] = `Basic ${this.authToken}`;
      }
    } else if (this.username && this.password) {
      // Generate Basic auth from username/password
      // Ensure we have valid non-empty strings
      const username = String(this.username).trim();
      const password = String(this.password).trim();
      if (username && password) {
        const credentials = Buffer.from(`${username}:${password}`).toString('base64');
        headers['Authorization'] = `Basic ${credentials}`;
      }
    }

    return headers;
  }

  /**
   * Execute an Elasticsearch query via Kibana's Console Proxy API
   * @param {string} index - Elasticsearch index pattern (e.g., 'my-index-*')
   * @param {Object} queryBody - Elasticsearch query body
   * @returns {Promise<Object>} Elasticsearch response
   */
  async search(index, queryBody) {
    const path = `/${index}/_search`;
    const url = `${this.baseUrl}/api/console/proxy?path=${encodeURIComponent(path)}&method=POST`;
    
    try {
      const response = await axios.post(url, queryBody, {
        headers: this.getAuthHeaders(),
      });
      return response.data;
    } catch (error) {
      if (error.response) {
        throw new Error(
          `Kibana API error: ${error.response.status} ${error.response.statusText}\n` +
          `Details: ${JSON.stringify(error.response.data, null, 2)}`
        );
      }
      throw error;
    }
  }

  /**
   * Check cluster health
   */
  async checkHealth() {
    const path = '/_cluster/health';
    const url = `${this.baseUrl}/api/console/proxy?path=${encodeURIComponent(path)}&method=GET`;
    
    try {
      const response = await axios.get(url, {
        headers: this.getAuthHeaders(),
      });
      return response.data;
    } catch (error) {
      if (error.response) {
        throw new Error(
          `Kibana API error: ${error.response.status} ${error.response.statusText}\n` +
          `Details: ${JSON.stringify(error.response.data, null, 2)}`
        );
      }
      throw error;
    }
  }
}
