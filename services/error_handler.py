"""
Universal Error Handler System - Modular and Reusable Error Handling
"""
import asyncio
import logging
import time
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Any, Optional, Tuple, Callable, Union, Type
import httpx
import requests


class ErrorType(Enum):
    """Base error types that can be extended by specific sources"""
    NETWORK_ERROR = "network_error"
    API_ERROR = "api_error"
    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    PARSING_ERROR = "parsing_error"
    PROXY_ERROR = "proxy_error"
    AUTHENTICATION_ERROR = "authentication_error"
    TOKEN_EXPIRED = "token_expired"
    UNKNOWN = "unknown"


@dataclass
class RetryConfig:
    """Base retry configuration that can be extended"""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    
    # Error-specific retry counts
    network_error_retries: int = 5
    rate_limit_retries: int = 3
    timeout_retries: int = 3
    proxy_error_retries: int = 2
    authentication_error_retries: int = 1
    token_expired_retries: int = 1
    api_error_retries: int = 0


@dataclass
class ErrorContext:
    """Base error context that can be extended"""
    identifier: str  # SKU, page_number, job_id, etc.
    attempt: int
    error_type: ErrorType
    error_message: str
    response_code: Optional[int] = None
    proxy_used: Optional[str] = None
    operation: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


class ErrorClassifier(ABC):
    """Abstract base class for error classification"""
    
    @abstractmethod
    def classify_error(self, exception: Exception, response_code: Optional[int] = None) -> ErrorType:
        """Classify the type of error"""
        pass


class DefaultErrorClassifier(ErrorClassifier):
    """Default error classifier that handles common HTTP and network errors"""
    
    def classify_error(self, exception: Exception, response_code: Optional[int] = None) -> ErrorType:
        """Classify the type of error for appropriate handling"""
        if isinstance(exception, httpx.TimeoutException):
            return ErrorType.TIMEOUT
        elif isinstance(exception, httpx.ProxyError):
            return ErrorType.PROXY_ERROR
        elif isinstance(exception, (httpx.NetworkError, httpx.ConnectError)):
            return ErrorType.NETWORK_ERROR
        elif isinstance(exception, requests.exceptions.Timeout):
            return ErrorType.TIMEOUT
        elif isinstance(exception, requests.exceptions.ConnectionError):
            return ErrorType.NETWORK_ERROR
        elif response_code:
            if response_code == 401:
                return ErrorType.AUTHENTICATION_ERROR
            elif response_code == 403:
                return ErrorType.TOKEN_EXPIRED
            elif response_code == 429:
                return ErrorType.RATE_LIMIT
            elif 400 <= response_code < 500:
                return ErrorType.API_ERROR
            elif response_code >= 500:
                return ErrorType.NETWORK_ERROR
        elif isinstance(exception, (ValueError, KeyError, TypeError)):
            return ErrorType.PARSING_ERROR
        else:
            return ErrorType.UNKNOWN


class RetryStrategy(ABC):
    """Abstract base class for retry strategies"""
    
    @abstractmethod
    def should_retry(self, error_context: ErrorContext) -> bool:
        """Determine if an error should be retried"""
        pass
    
    @abstractmethod
    def calculate_delay(self, attempt: int, error_type: ErrorType) -> float:
        """Calculate delay before retry"""
        pass


class DefaultRetryStrategy(RetryStrategy):
    """Default retry strategy with exponential backoff and jitter"""
    
    def __init__(self, config: RetryConfig):
        self.config = config
    
    def should_retry(self, error_context: ErrorContext) -> bool:
        """Determine if an error should be retried based on type and attempt count"""
        error_type = error_context.error_type
        attempt = error_context.attempt
        
        max_retries_for_type = {
            ErrorType.NETWORK_ERROR: self.config.network_error_retries,
            ErrorType.RATE_LIMIT: self.config.rate_limit_retries,
            ErrorType.TIMEOUT: self.config.timeout_retries,
            ErrorType.PROXY_ERROR: self.config.proxy_error_retries,
            ErrorType.AUTHENTICATION_ERROR: self.config.authentication_error_retries,
            ErrorType.TOKEN_EXPIRED: self.config.token_expired_retries,
            ErrorType.API_ERROR: getattr(self.config, 'api_error_retries', 0),  # Use configurable API error retries
            ErrorType.PARSING_ERROR: 0,  # Don't retry parsing errors
            ErrorType.UNKNOWN: self.config.max_retries
        }
        
        return attempt <= max_retries_for_type.get(error_type, 0)
    
    def calculate_delay(self, attempt: int, error_type: ErrorType) -> float:
        """Calculate delay before retry based on attempt and error type"""
        base_delay = self.config.base_delay
        
        # Different base delays for different error types
        if error_type == ErrorType.RATE_LIMIT:
            base_delay = 10.0  # Increased delay for rate limits (from 5.0 to 10.0)
        elif error_type == ErrorType.TOKEN_EXPIRED:
            base_delay = 3.0  # Medium delay for token issues
        elif error_type == ErrorType.PROXY_ERROR:
            base_delay = 2.0  # Medium delay for proxy errors
        elif error_type == ErrorType.AUTHENTICATION_ERROR:
            base_delay = 2.0  # Short delay for auth errors
        
        # Exponential backoff
        delay = min(
            base_delay * (self.config.exponential_base ** (attempt - 1)),
            self.config.max_delay
        )
        
        # Add jitter to prevent thundering herd
        if self.config.jitter:
            delay *= (0.5 + random.random() * 0.5)
        
        return delay


class ErrorLogger(ABC):
    """Abstract base class for error logging"""
    
    @abstractmethod
    def log_error(self, error_context: ErrorContext):
        """Log error with appropriate level and context"""
        pass
    
    @abstractmethod
    def log_success(self, identifier: str, attempt: int = 1, **kwargs):
        """Log successful operation"""
        pass


class DefaultErrorLogger(ErrorLogger):
    """Default error logger with structured logging"""
    
    def log_error(self, error_context: ErrorContext):
        """Log error with appropriate level and context"""
        log_message = (
            f"❌ {error_context.identifier} - Attempt {error_context.attempt} - "
            f"{error_context.error_type.value}: {error_context.error_message}"
        )
        
        if error_context.operation:
            log_message += f" (Operation: {error_context.operation})"
        
        if error_context.response_code:
            log_message += f" (HTTP {error_context.response_code})"
        
        if error_context.proxy_used:
            log_message += f" (Proxy: {error_context.proxy_used})"
        
        # Log at different levels based on error type
        if error_context.error_type in [ErrorType.PARSING_ERROR, ErrorType.API_ERROR]:
            logging.error(log_message)
        elif error_context.attempt > 1:
            logging.warning(log_message)
        else:
            logging.info(log_message)
    
    def log_success(self, identifier: str, attempt: int = 1, **kwargs):
        """Log successful operation"""
        log_message = f"✅ {identifier} succeeded"
        
        if attempt > 1:
            log_message += f" on attempt {attempt}"
        
        for key, value in kwargs.items():
            if value is not None:
                log_message += f" ({key}: {value})"
        
        logging.info(log_message)


class CircuitBreaker:
    """Circuit breaker pattern implementation"""
    
    def __init__(self, failure_threshold: int = 10, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def can_execute(self) -> bool:
        """Check if operation can be executed"""
        if self.state == "CLOSED":
            return True
        elif self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
                return True
            return False
        else:  # HALF_OPEN
            return True
    
    def record_success(self):
        """Record successful operation"""
        self.failure_count = 0
        self.state = "CLOSED"
    
    def record_failure(self):
        """Record failed operation"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"


class ErrorStats:
    """Error statistics tracking"""
    
    def __init__(self):
        self.total_errors = 0
        self.errors_by_type = {}
        self.retries_performed = 0
        self.successful_retries = 0
        self.failed_operations = []
    
    def update_stats(self, error_context: ErrorContext, retry_successful: bool = False):
        """Update error statistics"""
        self.total_errors += 1
        error_type_str = error_context.error_type.value
        self.errors_by_type[error_type_str] = (
            self.errors_by_type.get(error_type_str, 0) + 1
        )
        
        if error_context.attempt > 1:
            self.retries_performed += 1
            if retry_successful:
                self.successful_retries += 1
    
    def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive error summary"""
        total_operations = self.total_errors + self.successful_retries
        success_rate = (
            (total_operations - self.total_errors) / total_operations * 100
            if total_operations > 0 else 100
        )
        
        # Count response codes
        response_codes = {}
        for failed_op in self.failed_operations:
            if failed_op.get('response_code'):
                code = failed_op['response_code']
                response_codes[code] = response_codes.get(code, 0) + 1
        
        return {
            'total_operations': total_operations,
            'total_errors': self.total_errors,
            'success_rate_percent': round(success_rate, 2),
            'retries_performed': self.retries_performed,
            'successful_retries': self.successful_retries,
            'errors_by_type': self.errors_by_type,
            'response_codes': response_codes,
            'failed_operations': self.failed_operations
        }


class UniversalErrorHandler:
    """
    Universal error handler that can be configured for different sources
    """
    
    def __init__(
        self,
        retry_config: RetryConfig = None,
        error_classifier: ErrorClassifier = None,
        retry_strategy: RetryStrategy = None,
        error_logger: ErrorLogger = None,
        circuit_breaker: CircuitBreaker = None
    ):
        self.retry_config = retry_config or RetryConfig()
        self.error_classifier = error_classifier or DefaultErrorClassifier()
        self.retry_strategy = retry_strategy or DefaultRetryStrategy(self.retry_config)
        self.error_logger = error_logger or DefaultErrorLogger()
        self.circuit_breaker = circuit_breaker or CircuitBreaker()
        self.error_stats = ErrorStats()
    
    def classify_error(self, exception: Exception, response_code: Optional[int] = None) -> ErrorType:
        """Classify error using the configured classifier"""
        return self.error_classifier.classify_error(exception, response_code)
    
    def should_retry(self, error_context: ErrorContext) -> bool:
        """Check if should retry using the configured strategy"""
        return self.retry_strategy.should_retry(error_context)
    
    def calculate_delay(self, attempt: int, error_type: ErrorType) -> float:
        """Calculate delay using the configured strategy"""
        return self.retry_strategy.calculate_delay(attempt, error_type)
    
    def log_error(self, error_context: ErrorContext):
        """Log error using the configured logger"""
        self.error_logger.log_error(error_context)
    
    def log_success(self, identifier: str, attempt: int = 1, **kwargs):
        """Log success using the configured logger"""
        self.error_logger.log_success(identifier, attempt, **kwargs)
    
    def update_stats(self, error_context: ErrorContext, retry_successful: bool = False):
        """Update error statistics"""
        self.error_stats.update_stats(error_context, retry_successful)
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get error summary"""
        return self.error_stats.get_summary()
    
    async def execute_with_retry(
        self,
        operation: Callable,
        identifier: str,
        operation_name: Optional[str] = None,
        on_error: Optional[Callable[[], None]] = None,
        *args,
        **kwargs
    ) -> Tuple[str, Optional[Dict]]:
        """Execute an operation with retry logic"""
        attempt = 1
        last_error = None
        
        while True:
            # # Check circuit breaker
            # if not self.circuit_breaker.can_execute():
            #     raise Exception("Circuit breaker is OPEN")
            
            try:
                result = await operation(*args, **kwargs)
                
                # Record success in circuit breaker
                self.circuit_breaker.record_success()
                
                # If we had previous errors but this succeeded, log success
                if attempt > 1:
                    self.log_success(identifier, attempt, operation=operation_name)
                    self.update_stats(
                        ErrorContext(identifier, attempt, ErrorType.UNKNOWN, "retry_success", operation=operation_name),
                        retry_successful=True
                    )
                
                return result
                
            except Exception as e:
                response_code = None
                if hasattr(e, 'response') and isinstance(e.response, httpx.Response):
                    response_code = e.response.status_code

                error_type = self.classify_error(e, response_code)
                proxy = kwargs.get('proxy', {})
                error_context = ErrorContext(
                    identifier=identifier,
                    attempt=attempt,
                    error_type=error_type,
                    error_message=str(e),
                    response_code=response_code,
                    proxy_used=proxy.get('http') if isinstance(proxy, dict) else proxy,
                    operation=operation_name
                )
                
                self.log_error(error_context)
                self.update_stats(error_context)
                
                # Record failure in circuit breaker
                self.circuit_breaker.record_failure()
                
                # Trigger on_error callback for 4xx client errors
                if on_error and response_code and 400 < response_code < 500:
                    try:
                        await on_error()
                        logging.info(f"✅ on_error callback triggered for {identifier}")
                    except Exception as callback_error:
                        logging.error(f"Error in on_error callback: {callback_error}")
                
                # Check if we should retry
                if not self.should_retry(error_context):
                    self.error_stats.failed_operations.append({
                        'identifier': identifier,
                        'error_type': error_type.value,
                        'error_message': str(e),
                        'response_code': response_code,
                        'attempts': attempt
                    })
                    logging.error(f"❌ {identifier} failed after {attempt} attempts")
                    return identifier, None
                
                # Calculate delay and wait
                delay = self.calculate_delay(attempt, error_type)
                logging.info(f"⏳ Waiting {delay:.2f}s before retry {attempt + 1}")
                await asyncio.sleep(delay)
                
                attempt += 1
                last_error = e
    
    def execute_with_retry_sync(
        self,
        operation: Callable,
        identifier: str,
        operation_name: Optional[str] = None,
        on_error: Optional[Callable[[ErrorContext], None]] = None,
        *args,
        **kwargs
    ) -> Tuple[str, Optional[Dict]]:
        """Execute a synchronous operation with retry logic"""
        return asyncio.run(self.execute_with_retry(operation, identifier, operation_name, on_error, *args, **kwargs))


# Factory functions for creating error handlers with different configurations
def create_error_handler(
    source: str = "default",
    retry_config: RetryConfig = None,
    error_classifier: Type[ErrorClassifier] = DefaultErrorClassifier,
    retry_strategy: Type[RetryStrategy] = DefaultRetryStrategy,
    error_logger: Type[ErrorLogger] = DefaultErrorLogger
) -> UniversalErrorHandler:
    """
    Factory function to create error handlers with different configurations
    
    Args:
        source: Source name for configuration
        retry_config: Custom retry configuration
        error_classifier: Custom error classifier class
        retry_strategy: Custom retry strategy class
        error_logger: Custom error logger class
    
    Returns:
        UniversalErrorHandler: Configured error handler
    """
    
    # Source-specific configurations
    if source == "wayfair":
        retry_config = retry_config or RetryConfig(
            max_retries=3,
            network_error_retries=5,
            rate_limit_retries=5,  # Increased from 3 to 5 for rate limits
            timeout_retries=3,
            proxy_error_retries=2
        )
    elif source == "walmart":
        retry_config = retry_config or RetryConfig(
            max_retries=3,
            network_error_retries=5,
            rate_limit_retries=3,
            timeout_retries=3,
            proxy_error_retries=2,
            api_error_retries=0  # Don't retry API errors for Walmart
        )
    else:
        retry_config = retry_config or RetryConfig()
    
    # Create instances
    classifier_instance = error_classifier()
    strategy_instance = retry_strategy(retry_config)
    logger_instance = error_logger()
    
    return UniversalErrorHandler(
        retry_config=retry_config,
        error_classifier=classifier_instance,
        retry_strategy=strategy_instance,
        error_logger=logger_instance
    )


# Convenience functions for common use cases
def get_wayfair_error_handler() -> UniversalErrorHandler:
    """Get error handler configured for Wayfair"""
    return create_error_handler("wayfair")


def get_walmart_error_handler() -> UniversalErrorHandler:
    """Get error handler configured for Walmart"""
    return create_error_handler("walmart")


def get_sellercloud_error_handler() -> UniversalErrorHandler:
    """Get error handler configured for SellerCloud"""
    return create_error_handler("sellercloud")


def get_default_error_handler() -> UniversalErrorHandler:
    """Get default error handler"""
    return create_error_handler("default") 