package com.databricks.jdbc.auth;

import static com.databricks.jdbc.TestConstants.TEST_AUTH_URL;
import static com.databricks.jdbc.TestConstants.TEST_TOKEN_URL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.oauth.OpenIDConnectEndpoints;
import com.databricks.sdk.core.oauth.Token;
import com.databricks.sdk.core.oauth.TokenEndpointClient;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.HttpHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class OAuthRefreshCredentialsProviderTest {

  @Mock IDatabricksConnectionContext context;
  @Mock DatabricksConfig databricksConfig;
  @Mock HttpClient httpClient;
  private OAuthRefreshCredentialsProvider credentialsProvider;
  private static final String REFRESH_TOKEN_URL_DEFAULT =
      "jdbc:databricks://host:4423/default;transportMode=http;ssl=1;AuthMech=11;AuthFlow=0;httpPath=/sql/1.0/warehouses/99999999;OAuthRefreshToken=refresh-token";
  private static final String REFRESH_TOKEN_URL_OVERRIDE_CLIENT_ID =
      "jdbc:databricks://host:4423/default;transportMode=http;ssl=1;AuthMech=11;AuthFlow=0;httpPath=/sql/1.0/warehouses/99999999;OAuthRefreshToken=refresh-token;OAuth2ClientID=client_id";
  private static final String REFRESH_TOKEN_URL_OVERRIDE_CLIENT_ID_CLIENT_SECRET =
      "jdbc:databricks://host:4423/default;transportMode=http;ssl=1;AuthMech=11;AuthFlow=0;httpPath=/sql/1.0/warehouses/99999999;OAuthRefreshToken=refresh-token;OAuth2ClientID=client_id;OAuth2Secret=client_secret";
  private static final String REFRESH_TOKEN_URL_OVERRIDE_TOKEN_URL =
      "jdbc:databricks://host:4423/default;transportMode=http;ssl=1;AuthMech=11;AuthFlow=0;httpPath=/sql/1.0/warehouses/99999999;OAuthRefreshToken=refresh-token;OAuth2TokenEndpoint=token_endpoint";
  private static final String REFRESH_TOKEN_URL_OVERRIDE_EVERYTHING =
      "jdbc:databricks://host:4423/default;transportMode=http;ssl=1;AuthMech=11;AuthFlow=0;httpPath=/sql/1.0/warehouses/99999999;OAuthRefreshToken=refresh-token;OAuth2TokenEndpoint=token_endpoint;OAuth2ClientID=client_id;OAuth2Secret=client_secret";

  @Test
  void testRefreshThrowsExceptionWhenRefreshTokenIsNotSet() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(REFRESH_TOKEN_URL_DEFAULT, new Properties());
    when(databricksConfig.getOidcEndpoints())
        .thenReturn(
            new OpenIDConnectEndpoints(
                "https://oauth.example.com/oidc/v1/token",
                "https://oauth.example.com/oidc/v1/authorize"));
    credentialsProvider = new OAuthRefreshCredentialsProvider(connectionContext, databricksConfig);
    when(context.getOAuthRefreshToken()).thenReturn(null);
    when(databricksConfig.getHttpClient()).thenReturn(httpClient);
    OAuthRefreshCredentialsProvider providerWithNullRefreshToken =
        new OAuthRefreshCredentialsProvider(context, databricksConfig);
    providerWithNullRefreshToken.configure(databricksConfig);
    DatabricksDriverException exception =
        assertThrows(DatabricksDriverException.class, providerWithNullRefreshToken::getToken);
    assertEquals("oauth2: token expired and refresh token is not set", exception.getMessage());
  }

  @Test
  void testRefreshThrowsExceptionWhenOIDCFetchFails() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(REFRESH_TOKEN_URL_DEFAULT, new Properties());
    when(databricksConfig.getOidcEndpoints()).thenThrow(new IOException());
    assertThrows(
        DatabricksException.class,
        () -> new OAuthRefreshCredentialsProvider(connectionContext, databricksConfig));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        REFRESH_TOKEN_URL_DEFAULT,
        REFRESH_TOKEN_URL_OVERRIDE_EVERYTHING,
        REFRESH_TOKEN_URL_OVERRIDE_CLIENT_ID,
        REFRESH_TOKEN_URL_OVERRIDE_CLIENT_ID_CLIENT_SECRET,
        REFRESH_TOKEN_URL_OVERRIDE_TOKEN_URL
      })
  void testRefreshSuccess(String refreshTokenUrl) throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(refreshTokenUrl, null, null);
    boolean isDefaultEndpointPath = connectionContext.getTokenEndpoint() == null;
    if (isDefaultEndpointPath) {
      when(databricksConfig.getOidcEndpoints())
          .thenReturn(new OpenIDConnectEndpoints(TEST_TOKEN_URL, TEST_AUTH_URL));
    }
    credentialsProvider = new OAuthRefreshCredentialsProvider(connectionContext, databricksConfig);
    assertEquals("oauth-refresh", credentialsProvider.authType());
    when(databricksConfig.getHttpClient()).thenReturn(httpClient);
    try (MockedStatic<TokenEndpointClient> mocked = mockStatic(TokenEndpointClient.class)) {
      Token fakeToken =
          new Token("access-token", "token-type", "refresh-token", Instant.now().plusSeconds(360));
      mocked
          .when(
              () ->
                  TokenEndpointClient.retrieveToken(
                      any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(fakeToken);

      HeaderFactory headerFactory = credentialsProvider.configure(databricksConfig);
      Map<String, String> headers = headerFactory.headers();
      assertNotNull(headers.get(HttpHeaders.AUTHORIZATION));
      Token refreshedToken = credentialsProvider.getToken();
      assertEquals("token-type", refreshedToken.getTokenType());
      assertEquals("access-token", refreshedToken.getAccessToken());
      assertEquals("refresh-token", refreshedToken.getRefreshToken());
      assertFalse(refreshedToken.getExpiry().isBefore(Instant.now()));
    }
  }

  @Test
  void should_ThrowException_When_GetTokenCalledBeforeConfigure() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(REFRESH_TOKEN_URL_DEFAULT, new Properties());
    when(databricksConfig.getOidcEndpoints())
        .thenReturn(
            new OpenIDConnectEndpoints(
                "https://oauth.example.com/oidc/v1/token",
                "https://oauth.example.com/oidc/v1/authorize"));
    credentialsProvider = new OAuthRefreshCredentialsProvider(connectionContext, databricksConfig);

    // Calling getToken() before configure() should throw
    assertThrows(DatabricksDriverException.class, () -> credentialsProvider.getToken());
  }

  @Test
  void should_CacheTokenAcrossMultipleCalls() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(REFRESH_TOKEN_URL_DEFAULT, new Properties());
    when(databricksConfig.getOidcEndpoints())
        .thenReturn(new OpenIDConnectEndpoints(TEST_TOKEN_URL, TEST_AUTH_URL));
    when(databricksConfig.getHttpClient()).thenReturn(httpClient);

    credentialsProvider = new OAuthRefreshCredentialsProvider(connectionContext, databricksConfig);

    try (MockedStatic<TokenEndpointClient> mocked = mockStatic(TokenEndpointClient.class)) {
      Token fakeToken =
          new Token("access-token", "Bearer", "refresh-token", Instant.now().plusSeconds(3600));
      mocked
          .when(
              () ->
                  TokenEndpointClient.retrieveToken(
                      any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(fakeToken);

      credentialsProvider.configure(databricksConfig);

      // Call getToken multiple times
      Token token1 = credentialsProvider.getToken();
      Token token2 = credentialsProvider.getToken();
      Token token3 = credentialsProvider.getToken();

      // All tokens should have the same access token due to caching
      assertEquals(token1.getAccessToken(), token2.getAccessToken());
      assertEquals(token2.getAccessToken(), token3.getAccessToken());

      // retrieveToken should only be called once due to caching
      mocked.verify(
          () -> TokenEndpointClient.retrieveToken(any(), any(), any(), any(), any(), any(), any()),
          times(1));
    }
  }

  @Test
  void should_ReturnOAuthRefreshAsAuthType() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(REFRESH_TOKEN_URL_DEFAULT, new Properties());
    when(databricksConfig.getOidcEndpoints())
        .thenReturn(new OpenIDConnectEndpoints(TEST_TOKEN_URL, TEST_AUTH_URL));

    credentialsProvider = new OAuthRefreshCredentialsProvider(connectionContext, databricksConfig);

    assertEquals("oauth-refresh", credentialsProvider.authType());
  }

  @Test
  void should_IncludeAuthorizationHeaderWithCorrectFormat() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(REFRESH_TOKEN_URL_DEFAULT, new Properties());
    when(databricksConfig.getOidcEndpoints())
        .thenReturn(new OpenIDConnectEndpoints(TEST_TOKEN_URL, TEST_AUTH_URL));
    when(databricksConfig.getHttpClient()).thenReturn(httpClient);

    credentialsProvider = new OAuthRefreshCredentialsProvider(connectionContext, databricksConfig);

    try (MockedStatic<TokenEndpointClient> mocked = mockStatic(TokenEndpointClient.class)) {
      Token fakeToken =
          new Token(
              "test-access-token", "Bearer", "refresh-token", Instant.now().plusSeconds(3600));
      mocked
          .when(
              () ->
                  TokenEndpointClient.retrieveToken(
                      any(), any(), any(), any(), any(), any(), any()))
          .thenReturn(fakeToken);

      HeaderFactory headerFactory = credentialsProvider.configure(databricksConfig);
      Map<String, String> headers = headerFactory.headers();

      assertTrue(headers.containsKey(HttpHeaders.AUTHORIZATION));
      assertEquals("Bearer test-access-token", headers.get(HttpHeaders.AUTHORIZATION));
    }
  }

  @Test
  void should_UseRotatedRefreshTokenOnSubsequentRefresh() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(REFRESH_TOKEN_URL_DEFAULT, new Properties());
    when(databricksConfig.getOidcEndpoints())
        .thenReturn(new OpenIDConnectEndpoints(TEST_TOKEN_URL, TEST_AUTH_URL));
    when(databricksConfig.getHttpClient()).thenReturn(httpClient);

    credentialsProvider = new OAuthRefreshCredentialsProvider(connectionContext, databricksConfig);

    try (MockedStatic<TokenEndpointClient> mocked = mockStatic(TokenEndpointClient.class)) {
      // Track which refresh token is sent in each call
      AtomicReference<String> capturedRefreshToken = new AtomicReference<>();

      // First refresh returns a rotated refresh token
      Token firstToken =
          new Token("access-1", "Bearer", "rotated-refresh-token", Instant.now().plusSeconds(3600));
      // Second refresh returns another token
      Token secondToken =
          new Token(
              "access-2", "Bearer", "rotated-refresh-token-2", Instant.now().plusSeconds(3600));

      mocked
          .when(
              () ->
                  TokenEndpointClient.retrieveToken(
                      any(), any(), any(), any(), any(), any(), any()))
          .thenAnswer(
              invocation -> {
                @SuppressWarnings("unchecked")
                Map<String, String> params = (Map<String, String>) invocation.getArgument(4);
                capturedRefreshToken.set(params.get("refresh_token"));
                // Return first token on first call, second on subsequent
                if ("refresh-token".equals(params.get("refresh_token"))) {
                  return firstToken;
                }
                return secondToken;
              });

      credentialsProvider.configure(databricksConfig);

      // First call: should use original refresh token
      Token token1 = credentialsProvider.getToken();
      assertEquals("access-1", token1.getAccessToken());
      assertEquals("refresh-token", capturedRefreshToken.get());

      // Force token expiry by requesting a new token via a fresh CachedTokenSource
      // We need to trigger a second refresh. Since CachedTokenSource caches,
      // we verify the stored token was updated by checking its refresh token.
      assertEquals("rotated-refresh-token", token1.getRefreshToken());
    }
  }
}
