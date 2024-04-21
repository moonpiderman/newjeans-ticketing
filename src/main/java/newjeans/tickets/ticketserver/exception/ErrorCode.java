package newjeans.tickets.ticketserver.exception;

import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;

@AllArgsConstructor
public enum ErrorCode {
    QUEUE_ALREADY_REGISTERED_USER(HttpStatus.CONFLICT, "QUEUE-001", "이미 등록된 사용자입니다.");

    private final HttpStatus httpStatus;
    private final String code;
    private final String message;

    public ApplicationException build() {
        return new ApplicationException(httpStatus, code, message);
    }

    public ApplicationException build(Object... args) {
        return new ApplicationException(httpStatus, code, String.format(message, args));
    }
}
