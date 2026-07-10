import type { Metadata } from "next";
import { headers } from "next/headers";
import "./globals.css";

export async function generateMetadata(): Promise<Metadata> {
  const requestHeaders = await headers();
  const host = requestHeaders.get("x-forwarded-host") ?? requestHeaders.get("host") ?? "localhost:3000";
  const protocol = requestHeaders.get("x-forwarded-proto") ?? (host.startsWith("localhost") ? "http" : "https");
  const baseUrl = new URL(`${protocol}://${host}`);
  const title = "DevOps Note — 운영 지식을 내 것으로";
  const description =
    "Docker, Redis와 DevOps 운영 지식을 개념부터 트러블슈팅까지 단계별로 익히는 학습 사이트입니다.";

  return {
    metadataBase: baseUrl,
    title: { default: title, template: "%s | DevOps Note" },
    description,
    openGraph: {
      title,
      description,
      images: [{ url: new URL("/og.png", baseUrl).toString(), width: 1200, height: 630 }],
      locale: "ko_KR",
      type: "website",
    },
    twitter: {
      card: "summary_large_image",
      title,
      description,
      images: [new URL("/og.png", baseUrl).toString()],
    },
  };
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="ko">
      <body>{children}</body>
    </html>
  );
}
